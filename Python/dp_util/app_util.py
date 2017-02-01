import boto3
import botocore
import dp_util.constants as constants
import tkinter
import functools
import html.parser #included for installer purposes, do not remove
from tkinter import messagebox
from tkinter import filedialog
import dp_util.schema_util as schema_util
import dp_util.queue_util as queue_util
import dp_util.priority_util as priority_util
import dp_util.index_util as index_util
import dp_util.filing_util as filing_util

def establish_s3_connection():
    s3 = boto3.client('s3', config=botocore.client.Config(signature_version=botocore.UNSIGNED))

    # check that 990 database can be accessed
    exists = True
    try:
        s3.head_bucket(Bucket=constants.irs_bucket_name)
    except botocore.exceptions.ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            exists = False

    if not exists:
        exit(1)

    return s3


#todo: decorate confirmation dialog
#todo: progress bar
class Application(tkinter.Frame):
    #todo: detatch s3 and connection
    def __init__(self,master=None, s3 = None, connection = None):
        tkinter.Frame.__init__(self, master)
        self.s3 = s3
        self.connection = connection
        self.pack()
        self.create_widgets()
        self.priority_window = tkinter.Toplevel(self)
        self.priority_opt_frame = tkinter.Frame(self.priority_window)
        self.priority_opt_frame.grid(row=0, column=0)
        self.priority_button_frame = tkinter.Frame(self.priority_window)
        self.priority_button_frame.grid(row=1, column=0)
        self.priority_window.protocol("WM_DELETE_WINDOW", self.close_priority_window)
        self.priority_window.withdraw()
        self.priority_load_button = tkinter.Button(self.priority_button_frame, text="Load selected priorities", command=self.load_priorities)
        self.priority_run_button = tkinter.Button(self.priority_button_frame, text="Run selected priorities", command=self.run_priorities)
        self.priority_assign_button = tkinter.Button(self.priority_button_frame, text="Assign to groups", command=self.assign_priorities)
        self.priority_text_string_var = tkinter.StringVar()
        self.priority_text_field = tkinter.Entry(self.priority_button_frame, textvariable = self.priority_text_string_var)

        self.priority_load_button.grid(row=0, column=0)
        self.priority_run_button.grid(row=0, column=1)
        self.priority_assign_button.grid(row=0, column=2)
        self.priority_text_field.grid(row=1, column=1)
        self.priority_vars = []

    def confirm_populate_from_schema(self):
         if messagebox.askyesno(message='Are you sure you want to regenerate tables?',
                                icon='question', title='Confirm'):
            schema_util.populate_tables_from_schema(self.connection)

    def confirm_alter_from_schema(self):
         if messagebox.askyesno(message='Are you sure you want to alter tables?',
                                icon='question', title='Confirm'):
            schema_util.alter_tables(self.connection)

    def select_schema_file(self):
        schema_util.table_schema(self.connection, filedialog.askopenfilename(), '990', '2014', '5.0')

    def load_priorities(self):
        priority_list = self._poll_priorities()
        if len(priority_list) > 0:
            queue_util.load_queue_by_priority(self.connection, None, priority_list)

    def assign_priorities(self):
        priority_list = self._poll_priorities()
        if len(priority_list) > 0:
            table_name = self._fetch_priority_text()
            priority_util.add_priority_assignments(self.connection, table_name, priority_list)

    def _poll_priorities(self):
        priority_list = []
        for option in self.priority_vars:
            if option[0].get():
                priority_list.append(option[2])
        return priority_list

    def _fetch_priority_text(self):
        return self.priority_text_field.get()

    def run_priorities(self):
        priority_list = self._poll_priorities()
        if len(priority_list) > 0:
            queue_util.process_queue(self.s3, self.connection, no_upload=False,
                                     batch_size=10, priority_filter=priority_list, rerun_failed=False)

    def open_priority_window(self):
        self.priority_window.deiconify()
        self.generate_priority_grid()

    def close_priority_window(self):
        self.priority_window.withdraw()
        self.destroy_priority_grid()

    def generate_priority_grid(self):
        priority_list = priority_util.list_priorities(self.connection)
        for priority in priority_list:
            val_var = tkinter.IntVar()
            var_widget = tkinter.Checkbutton(self.priority_opt_frame, text=priority['name'], variable=val_var)
            var_widget.grid()
            self.priority_vars.append((val_var, var_widget, priority['id']))

    def destroy_priority_grid(self):
        for priority in self.priority_vars:
            print(priority[0].get())
            priority[1].destroy()
        self.priority_vars.clear()

    def create_widgets(self):
        self.check_index_update = tkinter.Button(self, text="Check for index update", command=functools.partial(index_util.check_index_update, self.s3))
        self.check_index_update.pack(side="bottom")

        self.upload_index = tkinter.Button(self, text="Upload index", command = functools.partial(index_util.upload_index, self.connection))
        self.upload_index.pack(side="bottom")
        self.pop_tables = tkinter.Button(self, text="Alter from schema",
                                   command=self.confirm_alter_from_schema)
        self.pop_tables.pack(side="bottom")
        self.table_schema = tkinter.Button(self, text="Load new schema", command = self.select_schema_file)
        self.table_schema.pack(side="bottom")

        self.fill_queue = tkinter.Button(self, text="Fill queue from index",
                                         command=functools.partial(queue_util.load_queue_random, self.connection, 100))
        self.fill_queue.pack(side="bottom")
        self.process_queue = tkinter.Button(self, text="Process queue",
                                            command = functools.partial(queue_util.process_queue, self.s3, self.connection, no_upload = False, batch_size = 10, priority_filter = None, rerun_failed = False))
        self.process_queue.pack(side="bottom")
        self.priority_open = tkinter.Button(self, text="Priorities", command = self.open_priority_window)
        self.priority_open.pack(side="bottom")
        self.run_test_button = tkinter.Button(self, text="Run test", command = functools.partial(filing_util.test_filing, self.s3, self.connection, 1200))
        self.run_test_button.pack(side="bottom")
        self.digest_index_button = tkinter.Button(self, text="Digest index file", command=index_util.digest_index_file)
        self.digest_index_button.pack(side="bottom")
        self.upload_digest_button = tkinter.Button(self, text="Upload index digest", command = functools.partial(index_util.upload_digested_file, self.connection))
        self.upload_digest_button.pack(side="bottom")