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

import argparse
import logging
import sys
from datetime import datetime

from . import common
from .api import entrypoint

MINIMUM_PYTHON_VERSION = (3, 5)
if hasattr(sys, '_real_argv'):
    sys.argv[0] = sys._real_argv


def parseConfig(item):
    try:
        key, value = item.split('=')
        return key, value
    except ValueError:
        die("Invalid configuration format: %s" % item)


def parseArgs():
    LogLevelNames = [n.lower() for n in logging._nameToLevel]

    parser = argparse.ArgumentParser(fromfile_prefix_chars='@')

    parser.add_argument(
        '--iterations',
        type=int,
        default=1,
        help="number of times to run the program, defaults to 1.",
    )
    parser.add_argument(
        "--no-log",
        action="store_true",
        default=False,
        help="if set, don't customize the root logger. "
        "Useful if DistAlgo is run as a library .",
    )
    parser.add_argument(
        "-f",
        "--logfile",
        action="store_true",
        default=False,
        help="creates a log file for this run.",
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "-T",
        "--record-trace",
        action="store_true",
        default=False,
        help="stores a message trace for each process.",
    )
    group.add_argument(
        "-Y",
        "--replay-traces",
        nargs='+',
        default=[],
        help="replays the recorded message traces "
        "instead of running the `main` method. ",
    )

    parser.add_argument(
        "-P",
        "--dump-trace",
        action='store_true',
        default=False,
        help="print the contents of DistAlgo trace files. "
        "The files to print are specified by '--replay_traces'.",
    )
    parser.add_argument(
        "-Sm",
        "--substitute-modules",
        nargs='+',
        default=[],
        help="a list of ORIGMOD:NEWMOD pairs, such that all "
        "references to ORIGMOD will be replaced by NEWMOD "
        "when unpickling messages or replaying traces. ",
    )
    parser.add_argument(
        "-Sc",
        "--substitute-classes",
        nargs='+',
        default=[],
        help="a list of ORIGCLS:NEWCLS pairs, such that all "
        "references to ORIGCLS will be replaced by NEWCLS "
        "when unpickling messages or replaying traces. ",
    )

    parser.add_argument(
        "--logfilename",
        help="file name of the log file, defaults to appending"
        "'.log' to the source file name.",
    )
    parser.add_argument(
        "--logdir",
        default=datetime.now().strftime('%Y-%m-%d_%H%M%S'),
        help="directory to store log and trace files, "
        "can be absolute or relative to the current working "
        "directory. Default value is a subdirectory under "
        "the working directory corresponding to the current "
        "date and time.",
    )
    parser.add_argument(
        "-L",
        "--logconsolelevel",
        choices=LogLevelNames,
        default="info",
        help="severity level of logging messages to print to "
        "the console, defaults to 'info'.",
    )
    parser.add_argument(
        "-F",
        "--logfilelevel",
        choices=LogLevelNames,
        default="debug",
        help="severity level of logging messages to log to "
        "the log file, defaults to 'debug'.",
    )
    parser.add_argument(
        "--pid-format",
        choices=['short', 'long', 'full'],
        default='short',
        help="sets the format of string representation of "
        "process ids. 'short' prints the process class name "
        "followed by the uid truncated to the last 5 hexdigits. "
        "This is the default. 'long' prints the process class "
        "name followed by the full 24 hexdigit untruncated uid. "
        "'full' prints the full string representation of the "
        "process id object. For named processes, both the "
        "'short' and 'long' forms print the process name "
        "in place of the uid.",
    )
    parser.add_argument(
        "-i",
        "--load-inc-module",
        action="store_true",
        default=False,
        help="if set, try to load the incrementalized " "interface module.",
    )
    parser.add_argument(
        "-C",
        "--control-module-name",
        default=None,
        help="name of the control module. If set, "
        "results from the inc-module will be compared "
        "against results from this module. Any mismatch will "
        "raise IntrumentationError. Defaults to no control "
        "module.",
    )
    parser.add_argument(
        "--inc-module-name",
        help="name of the incrementalized interface module, "
        "defaults to source module name + '_inc'. ",
    )
    parser.add_argument(
        "-H",
        "--hostname",
        default=None,
        help="hostname for binding network sockets, "
        "default value is the fully qualified domain "
        "name (FQDN) of the "
        "running host if '--nodename' is set, or 'localhost' "
        "if '--nodename' is not set.",
    )
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=None,
        help="port number for binding network sockets. "
        "Default is bind to any random available port. "
        "This option is ignored if '--nodename' is not set.",
    )
    parser.add_argument(
        "-n",
        "--nodename",
        default="",
        help="mnemonic name for this DistAlgo node process. " "Default is no name. ",
    )
    parser.add_argument(
        "-R",
        "--peer",
        default="",
        help="address of an existing node, used for bootstrapping "
        "this node. An address should be of the "
        "form HOSTNAME[:PORT], where 'PORT' defaults to the "
        "value of '--default-master-port' if omitted. "
        "This option is ignored if '--nodename' is not set, "
        "or if '--master' is set.",
    )
    parser.add_argument(
        "--default-master-port",
        type=int,
        default=15000,
        help="default port number for connecting to remote "
        "nodes if the port number is not explicitly given. "
        "Default value is 15000. "
        "This option is ignored if '--nodename' is not set.",
    )
    parser.add_argument(
        "--min-port",
        type=int,
        default=10000,
        help="low end of the port range for binding network "
        "sockets. "
        "Default value is 10000. "
        "This option has no effect if '--port' is set.",
    )
    parser.add_argument(
        "--max-port",
        type=int,
        default=60000,
        help="high end of the port range for binding network "
        "sockets. "
        "Default value is 60000. "
        "This option has no effect if '--port' is set.",
    )
    parser.add_argument(
        "--cookie",
        default=None,
        help="a string for authentication of peers. "
        "All peer processes participating in message passing "
        "must have matching cookies. "
        "If this option is not set, but the file "
        "'${HOME}/.da.cookie' exists, then the contents of "
        "that file will be used as the "
        "cookie. If this option is not set and the file "
        "'${HOME}/.da.cookie' does not exist, then peer "
        "authentication will be disabled for this node.",
    )
    parser.add_argument(
        '--message-buffer-size',
        type=int,
        default=(4 * 1024),
        help="size in bytes of the buffers used to serialize "
        "messages. The serialized(pickled) size of any DistAlgo "
        "message must be smaller than this value in order for the "
        "system to be able to send it "
        "across address space boundaries. Default value is 4KB.",
    )
    parser.add_argument(
        "--tcp-dont-reuse-addr",
        help="if set, the system will not bind to TCP ports "
        "that are in the 'TIME_WAIT' state. ",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "-M",
        "--master",
        help="if set, the system will not try to bootstrap " "on startup. ",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "-D",
        "--idle",
        help="if set, this node will run as an idle node, "
        "i.e. it will not execute the `main` method defined "
        "in the main module. ",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "-r",
        "--recompile",
        dest="recompile",
        help="force recompile DistAlgo source file. ",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "-c",
        "--compiler-flags",
        type=str,
        default="",
        help="flags to pass to the compiler, if (re)compiling " "is required.",
    )
    parser.add_argument(
        "-o",
        "--config",
        default=[],
        nargs='*',
        help="sets runtime configuration variables, overrides "
        "configurations declared in the program source.",
    )
    parser.add_argument(
        "--start-method",
        default=None,
        choices=['fork', 'spawn'],
        help="choose the semantics for creating child process."
        " 'fork' is the default method on UNIX-like systems,"
        " 'spawn' is the default method on Windows systems.",
    )
    parser.add_argument(
        "-I",
        "--default-proc-impl",
        default='process',
        choices=['process', 'thread'],
        help="choose the default implementation for running "
        " DistAlgo processes."
        " 'process' uses OS processes,"
        " 'thread' uses OS threads.",
    )
    parser.add_argument(
        '--rules', default=False, help="Enable Rules", action='store_true'
    )
    parser.add_argument(
        '--constraints',
        default=False,
        help="Enable Constraint Solving",
        action='store_true',
    )
    parser.add_argument(
        '--timing',
        default=False,
        help="Enable dispaly of timing of xsb",
        action='store_true',
    )
    parser.add_argument("-v", "--version", action="version", version=common.__version__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "-m",
        "--module",
        default=None,
        nargs=argparse.REMAINDER,
        help="name of a DistAlgo module that will be run as "
        "the main module. If this argument is specified, "
        "all command line options after this point will be "
        "passed to the specified module in `sys.argv`.",
    )
    group.add_argument(
        "-B",
        "--help-builtins",
        action='store_true',
        default=False,
        help="print a list of DistAlgo built-in functions and " "exit.",
    )
    group.add_argument("file", nargs='?', help="DistAlgo source file to run.")
    parser.add_argument(
        "args",
        nargs=argparse.REMAINDER,
        help="arguments passed to program in sys.argv[1:].",
    )

    args = parser.parse_args()
    if args.help_builtins:
        return help_builtins()
    elif not args.idle and args.module is None and args.file is None:
        parser.print_usage()
        return 1
    elif not args.max_port > args.min_port:
        die("'max_port' must be greater than 'min_port'!")
    args.config = dict(parseConfig(item) for item in args.config)
    if args.cookie is not None:
        args.cookie = args.cookie.encode()
    return args


def help_builtins():
    from inspect import signature

    print("======= DistAlgo Builtin Functions =======\n")
    for fname in sorted(common.builtin_registry):
        func = common.builtin_registry[fname]
        sig = ", ".join([arg for arg in signature(func).parameters if arg != 'self'])
        print("{}({}):\n\t{}\n".format(fname, sig, func.__doc__))
    return 0


def libmain():
    """Main program entry point.

    Parses command line options, sets up global variables, and calls the 'main'
    function of the DistAlgo program.

    """
    if sys.version_info < MINIMUM_PYTHON_VERSION:
        die(
            "DistAlgo requires Python version {} or newer.\n".format(
                MINIMUM_PYTHON_VERSION
            )
        )

    args = parseArgs()
    if isinstance(args, int):
        return args
    else:
        try:
            common.global_init(args.__dict__)
        except common.ConfigurationError as e:
            die(repr(e))
        return entrypoint()


def die(mesg=None):
    if mesg != None:
        sys.stderr.write(mesg + "\n")
    sys.exit(10)


if __name__ == '__main__':
    sys.exit(libmain())
