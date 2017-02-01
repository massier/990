import subprocess
import simplejson as json


class PHP:
    """This class provides a stupid simple interface to PHP code."""

    def __init__(self, prefix="", postfix=""):
        """prefix = optional prefix for all code (usually require statements)
        postfix = optional postfix for all code
        Semicolons are not added automatically, so you'll need to make sure to put them in!"""

        self.prefix = prefix
        self.postfix = postfix

    def __submit(self, code):
        p = subprocess.Popen("php -f " + code, shell=True, stdout=subprocess.PIPE)
        #(out, inp) = (p.stdout, p.stdin)
        #inp.write(bytes("<? ", "utf-8"))
        #inp.write(bytes(self.prefix, "utf-8"))
        #inp.write(bytes(code, "utf-8"))
        #inp.write(bytes(self.postfix, "utf-8"))
        #inp.write(bytes(" ?>", "utf-8"))
        #inp.close()
        return p.stdout

    def get_raw(self, code):
        """Given a code block, invoke the code and return the raw result as a string."""
        out = self.__submit(code)
        return out.read()

    def get(self, code):
        """Given a code block that emits json, invoke the code and interpret the result as a Python value."""
        out = self.__submit(code)
        return out.read()
        return json.loads(out.read())

    def get_one(self, code):
        """Given a code block that emits multiple json values (one per line), yield the next value."""
        out = self.__submit(code)
        for line in out:
            line = line.strip()
            if line:
                yield json.loads(line)


def test_php():
    php = PHP()
    print(php.get("test.php"))