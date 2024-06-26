# Copyright (c) 2010-2017 Bo Lin
# Copyright (c) 2010-2017 Yanhong Annie Liu
# Copyright (c) 2010-2017 Stony Brook University
# Copyright (c) 2010-2017 The Research Foundation of SUNY
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation files
# (the "Software"), to deal in the Software without restriction,
# including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import ast
import io
import sys

import da
from da.tools.unparse import Unparser

from .. import common
from . import dast

DB_ERROR = 0
DB_WARN = 1
DB_INFO = 2
DB_DEBUG = 3
Debug = DB_ERROR

VERSION_HEADER = "# -*- generated by {} -*-"


KW_RULES = "rules_"
KW_INFER = "infer"
KW_CONSTRAINT = "csp_"
KW_QUERY = "query"


##########
# Exceptions:
class CompilerException(Exception):
    def __init__(self, reason=None, node=None, name=None, msg=None):
        super().__init__(reason, node, name, msg)
        self.reason = reason
        self.node = node
        self.name = name
        self.msg = msg


class MalformedStatementError(CompilerException):
    pass


class ResolverException(CompilerException):
    pass


def to_source(tree):
    textbuf = io.StringIO(newline='')
    # textbuf.write(VERSION_HEADER.format(da.__version__))
    if sys.version_info < (3, 9):
        Unparser(tree, textbuf)
        return textbuf.getvalue()
    else:
        return ast.unparse(tree)


def to_file(tree, fd):
    if sys.version_info < (3, 9):
        fd.write(VERSION_HEADER.format(da.__version__))
        return Unparser(tree, fd).counter
    else:
        output = ast.unparse(tree)
        fd.write(output)
        return len(output)


def set_debug_level(level):
    global Debug
    if is_valid_debug_level(level):
        Debug = level


def get_debug_level():
    return Debug


def is_valid_debug_level(level):
    return type(level) is int and DB_ERROR <= level and level <= DB_DEBUG


# Common utility functions


def printe(mesg, lineno=0, col_offset=0, filename="", outfd=sys.stderr):
    if Debug >= DB_ERROR:
        fs = "%s:%d:%d: error: %s"
        print(fs % (filename, lineno, col_offset, mesg), file=outfd)


def printw(mesg, lineno=0, col_offset=0, filename="", outfd=sys.stderr):
    if Debug >= DB_WARN:
        fs = "%s:%d:%d: warning: %s"
        print(fs % (filename, lineno, col_offset, mesg), file=outfd)


def printd(mesg, lineno=0, col_offset=0, filename="", outfd=sys.stderr):
    if Debug >= DB_DEBUG:
        fs = "%s:%d:%d: DEBUG: %s"
        print(fs % (filename, lineno, col_offset, mesg), file=outfd)


def printi(mesg, lineno=0, col_offset=0, filename="", outfd=sys.stdout):
    if Debug >= DB_INFO:
        fs = "%s:%d:%d: %s"
        print(fs % (filename, lineno, col_offset, mesg), file=outfd)


class Namespace:
    """A simple container for storing arbitrary attributes."""

    pass


class OptionsManager:
    def __init__(self, cmdline_args, module_args, default=False):
        self.cmdline_args = cmdline_args
        self.module_args = module_args
        self.default = default

    def __getattribute__(self, option):
        if option in {'cmdline_args', 'module_args', 'default'}:
            return super().__getattribute__(option)

        if hasattr(self.cmdline_args, option):
            return getattr(self.cmdline_args, option)
        elif hasattr(self.module_args, option):
            return getattr(self.module_args, option)
        else:
            return self.default


class CompilerMessagePrinter:

    def __init__(self, filename, _parent=None):
        self.filename = filename
        self._parent = _parent
        if not _parent:
            self._errcnt = 0
            self._warncnt = 0
        else:
            assert isinstance(_parent, CompilerMessagePrinter)

    def incerr(self):
        if self._parent:
            self._parent.incerr()
        else:
            self._errcnt += 1

    def incwarn(self):
        if self._parent:
            self._parent.incwarn()
        else:
            self._warncnt += 1

    @property
    def errcnt(self):
        if self._parent:
            return self._parent.errcnt
        else:
            return self._errcnt

    @property
    def warncnt(self):
        if self._parent:
            return self._parent.warncnt
        else:
            return self._warncnt

    def accumulate_counters(self, printer):
        """Add the counter values from `printer` into ours."""
        assert isinstance(printer, CompilerMessagePrinter)
        if self._parent:
            self._parent.accumulate_counters(printer)
        else:
            self._errcnt += printer.errcnt
            self._warncnt += printer.warncnt

    def error(self, mesg, node):
        self.incerr()
        if node is not None:
            printe(mesg, node.lineno, node.col_offset, self.filename)
        else:
            printe(mesg, 0, 0, self.filename)

    def find_function(self, node, name):
        if not node:
            return False
        if isinstance(node, dast.Function):
            if node.name.startswith(name):
                # if node.name == name:
                return True
            else:
                return False
        else:
            return self.find_function(node.parent, name)

    def test_parent(self, node):
        if common.get_runtime_option('rule', default=False) and self.find_function(
            node, KW_RULES
        ):
            return True
        if common.get_runtime_option(
            'constraint', default=False
        ) and self.find_function(node, KW_CONSTRAINT):
            return True
        return False

    def find_call(self, node):
        if not node:
            return False
        if isinstance(node, dast.CallExpr):
            return True
        else:
            return self.find_call(node.parent)

    def test_funccall(self, node, parent_node):
        if isinstance(node, ast.Name) and self.find_call(parent_node):
            if common.get_runtime_option('rule', default=False) and node.id == KW_INFER:
                return True
            # if common.get_runtime_option('constraint', default=False) and node.id == KW_CONSTRAINT:
            #     return True
        return False

    def warn(self, mesg, node, parent_node=None):
        if parent_node:
            if self.test_parent(parent_node) or self.test_funccall(node, parent_node):
                return
        self.incwarn()
        if node is not None:
            printw(mesg, node.lineno, node.col_offset, self.filename)
        else:
            printw(mesg, 0, 0, self.filename)

    def debug(self, mesg, node=None):
        if node is not None:
            printd(mesg, node.lineno, node.col_offset, self.filename)
        else:
            printd(mesg, 0, 0, self.filename)
