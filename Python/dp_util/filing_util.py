import dp_util.constants as constants
import dp_util.db_util as db_util
import dp_util.batch_insert as batch_insert
import os
from collections import OrderedDict
from lxml import etree as et


def filename_from_url(url):
    return url.split('/')[-1]


# Note: also works with bare or pathed local files
# Todo: make non-blocking version to parallelize download with processing what we have?
def download_filing(client, url):
    filename = filename_from_url(url)
    client.download_file(constants.irs_bucket_name, filename, constants.scratch_folder + filename)


def delete_filing(url):
    filename = filename_from_url(url)
    try:
        os.remove(constants.scratch_folder + filename)
    except PermissionError:
        print("Could not remove file")


def load_xml(filename):
    return et.parse(constants.scratch_folder + filename)


def get_version(tree):
    root = tree.getroot()
    version = root.get('returnVersion')
    return version.split('v')


def get_form_type(tree):
    root = tree.getroot()
    return root.find('ns:ReturnHeader', constants.xml_ns).find('ns:ReturnTypeCd', constants.xml_ns).text


def discover_node_status(tree, path):
    root = tree.getroot()
    nsdict = {"default": root.nsmap[None]}
    path = './/default:' + path.split('/', 1)[1].replace('/', '/default:')
    if root.find(path, nsdict) is not None:
        return constants.NodeStatus.element
    new_path_parts = path.rsplit('/', 1)
    attr_path = '/'.join(new_path_parts[:-1])
    parent_node = root.findall(attr_path, nsdict)
    for child in parent_node:
        attr = child.get(path.rsplit(':', 1)[-1])
        if attr:
            return constants.NodeStatus.attribute
    return constants.NodeStatus.not_found


def get_all_from_path(tree, path, node_type = None):
    root = tree.getroot()
    nsdict = {"default": root.nsmap[None]}
    path = './/default:' + path.split('/', 1)[1].replace('/', '/default:')
    if node_type is None:
        result = root.findall(path, nsdict)

        if len(result) == 0:
            new_path_parts = path.rsplit('/',1)
            attr_path = '/'.join(new_path_parts[:-1])
            parent_node = root.findall(attr_path, nsdict)
            result_list = []
            for child in parent_node:
                attr = child.get(path.rsplit(':', 1)[-1])
                if attr:
                    result_list.append(attr)
            return result_list
        else:
            return result
    elif node_type is constants.NodeStatus.element:
        return root.findall(path, nsdict)
    elif node_type is constants.NodeStatus.attribute:
        new_path_parts = path.rsplit('/', 1)
        attr_path = '/'.join(new_path_parts[:-1])
        parent_node = root.findall(attr_path, nsdict)
        result_list = []
        for child in parent_node:
            attr = child.get(path.rsplit(':', 1)[-1])
            if attr:
                result_list.append(attr)
        return result_list
    else:
        print("Invalid node type: " + node_type)
        return None


# attempts to follow a unique path down the xml tree, using the values in decisions to pick branches
def probe_xml_branch(root, path, decisions, node_type):
    nsdict = {"default": root.nsmap[None]}
    decision_idx = 0
    path_parts = path.split('/')[1:]
    curr_node = root
    for part in path_parts:
        if part is path_parts[-1] and node_type is constants.NodeStatus.attribute:
            if curr_node.get(part):
                return True
            return False
        else:
            elem_list = curr_node.findall('default:' + part, nsdict)
            if len(elem_list) > 1:
                curr_node = elem_list[decisions[decision_idx]]
                decision_idx += 1
            elif len(elem_list) == 1:
                curr_node = elem_list[0]
            else:
                return False

    return True


def inspect_prefixes(decision_dict):
    """
    Evaluate whether any of the keys in the dict are prefixes of other keys
    """
    #print("MUST INSPECT")
    pass


def enumerate_xml_branches(root, path, node_type):
    """
    Returns a (path_tuple)->val dictionary
    """
    nsdict = {"default": root.nsmap[None]}
    path_parts = path.split('/')[1:]
    valid_paths = OrderedDict()
    enumerate_subtree(root, path_parts, (), node_type, valid_paths)
    return valid_paths


def enumerate_subtree(subroot, path_parts, prefix, node_type, valid_paths):
    nsdict = {"default": subroot.nsmap[None]}
    if len(path_parts) > 1:
        child_list = subroot.findall('default:' + path_parts[0], nsdict)
        if len(child_list) > 1:
            counter = 0
            for child in child_list:
                enumerate_subtree(child, path_parts[1:], prefix + (counter, ), node_type, valid_paths)
                counter += 1
        elif len(child_list) == 1:
            enumerate_subtree(child_list[0], path_parts[1:], prefix, node_type, valid_paths)
    elif len(path_parts) == 1:
        if node_type is constants.NodeStatus.element:
            child_list = subroot.findall('default:' + path_parts[0], nsdict)
            if len(child_list) == 1:
                if prefix in valid_paths:
                    print("Warning: dupe")
                valid_paths[prefix] = child_list[0].text
            else:
                counter = 0
                for child in child_list:
                    if prefix + (counter,) in valid_paths:
                        print("Warning: dupe")
                    valid_paths[prefix + (counter,)] = child.text
                    counter += 1
        elif node_type is constants.NodeStatus.attribute:
            attr_val = subroot.get(path_parts[0])
            if attr_val:
                if prefix in valid_paths:
                    print("Warning: dupe")
                valid_paths[prefix] = attr_val
        else:
            print("SHOULD NOT BE PASSING UNKNOWN TYPES")
    else:
        print("WARNING: reached 0-length path")

'''
def probe_sub_tree(root, subpath_parts):
    nsdict = {"default": root.nsmap[None]}
    curr_node = root
    elem_list = curr_node.findall('default:' + subpath_parts[0], nsdict)
    if len(elem_list)


def map_xml_branches(root, path):
    nsdict = {"default": root.nsmap[None]}
    decision_idx = 0
    path_parts = path.split('/')[1:]
    curr_node = root
    for part in path_parts:
        elem_list = curr_node.findall('default:' + part, nsdict)
        if len(elem_list) > 1:
            curr_node = elem_list[decisions[decision_idx]]
            decision_idx += 1
        elif len(elem_list) == 1:
            curr_node = elem_list[0]
        else:
            return False

    return True
'''


def encode_decision(decision):
    row_code = 0
    place_mult = 1
    for branch in reversed(decision):
        row_code += branch * place_mult
        place_mult *= 100

    return row_code

def debug_schema_cursor(cursor, query, vals = None):
    if vals:
        cursor.execute(query, vals)
    else:
        cursor.execute(query)


# todo: bring in defusedxml
def process_xml(tree, connection, filing_id, form_type_in = None, no_upload = False, batch = None):
    """
    NOTE: elements will end up on the same row if the same
     decision string can be used to describe both.  This
     may be incorrect in the case where the branching occurs on the element rather than its parents
    """
    version_year, version_num = get_version(tree)
    form_type = form_type_in if form_type_in is not None else get_form_type(tree)
    root = tree.getroot()

    schema_query = "SELECT * FROM irs.schema WHERE year = %s AND version = %s AND form = %s AND !ISNULL(dataType)"
    schema_cursor = connection.cursor()
    debug_schema_cursor(schema_cursor, schema_query, (version_year, version_num, form_type))

    if schema_cursor.rowcount == 0:
        print("Error: Form %s, Year %s, Version %s is not handled" % (form_type, version_year, version_num))
        return 0
    else:
        print("Form %s, Year %s, Version %s" % (form_type, version_year, version_num))

    data = {}
    schema = list(schema_cursor.fetchall())
    all_entries = []
    for row in schema:
        table_name = row['tableName']
        if table_name not in all_entries:
            all_entries.append(table_name)
        node_status = discover_node_status(tree, row['path'])
        if node_status == constants.NodeStatus.not_found:
            continue

        if False:
            if table_name not in data:
                data[table_name] = []
            val = get_all_from_path(tree, row['path'], node_status)
            counter = 0
            for subval in val:
                try:
                    while not probe_xml_branch(root, row['path'], (counter,), node_type=node_status):
                        counter += 1
                except Exception as err:
                    print("Exception in branch probe")
                    return 0
                while len(data[table_name]) <= counter:
                    data[table_name].append({})
                subval_text = subval.text if node_status is constants.NodeStatus.element else subval
                if subval_text:
                    v = subval_text.strip()
                    if len(v) > 0:
                        data[table_name][counter][row['fieldName']] = v

                counter += 1

        else:
            if table_name not in data:
                data[table_name] = OrderedDict()
            val = enumerate_xml_branches(tree.getroot(), row['path'], node_status)

            for decision, subval in val.items():
                if decision not in data[table_name]:
                    data[table_name][decision] = {}
                subval_text = subval
                if subval_text:
                    v = subval_text.strip()
                    if len(v) > 0:
                        data[table_name][decision][row['fieldName']] = v

    if batch is not None:
        insert_cursor = connection.cursor()
    try:
        for table, entry in data.items():
            inspect_prefixes(entry)
            for table_row, subentry in entry.items():
                if len(subentry) > 0:
                    subentry['fk'] = filing_id
                    subentry['row'] = encode_decision(table_row)
                    if batch is None and not no_upload:
                        db_util.insert_into_table(table, [subentry], connection, insert_cursor)

                    elif batch is not None:
                        batch.add_values(table, subentry, connection)
    except:
        print("Exception in entry insertion")
        if batch is None:
            connection.rollback()
        return 0
    else:
        if batch is None:
            connection.commit()
    print("done")
    return 1


def test_filing(client, connection, index_id):
    batch = batch_insert.BatchInsert()
    index_cursor = connection.cursor()
    index_query = "SELECT idx.* FROM irs.form_index idx WHERE idx.id = %s"
    index_cursor.execute(index_query, (index_id,))
    index_block = index_cursor.fetchall()
    index_cursor.close()
    for row in index_block:
        download_filing(client, row['url'])
        tree = load_xml(filename_from_url(row['url']))
        version_year, version_num = get_version(tree)
        form_type = row['formType']
        success = process_xml(tree, connection, row['id'], form_type_in = form_type, no_upload = True, batch = batch)

        delete_filing(filename_from_url(row['url']))
