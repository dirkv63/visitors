"""
This module consolidates all local configuration for the script, including modulename collection for logfile name
setup and initializing the config file.
Also other utilities find their home here.
"""

import configparser
import logging
import logging.handlers
import os
import platform
import sys
import subprocess
from collections import namedtuple
from datetime import datetime

def init_env(projectname, filename):
    """
    This function will initialize the environment: Find and return handle to config file and set-up logging.
    :param projectname: Name that will be used to find ini file in properties subdirectory.
    :param filename: Filename (__file__) of the calling script (for logfile).
    :return: config handle
    """
    projectname = projectname
    modulename = get_modulename(filename)
    config = get_inifile(projectname)
    # my_log = init_loghandler(config, modulename)
    init_loghandler(config, modulename)
    # my_log.info('Start Application')
    return config


def get_modulename(scriptname):
    """
    Modulename is required for logfile and for properties file.
    :param scriptname: Name of the script for which modulename is required. Use __file__.
    :return: Module Filename from the calling script.
    """
    # Extract calling application name
    (filepath, filename) = os.path.split(scriptname)
    (module, fileext) = os.path.splitext(filename)
    return module


def init_loghandler(config, modulename):
    """
    This function initializes the loghandler. Logfilename consists of calling module name + computername.
    Logfile directory is read from the project .ini file.
    Format of the logmessage is specified in basicConfig function.
    This is for Log Handler configuration. If basic log file configuration is required, then use init_logfile.
    :param config: Reference to the configuration ini file. Directory for logfile should be
    in section Main entry logdir.
    :param modulename: The name of the module. Each module will create it's own logfile.
    :return: Log Handler
    """
    logdir = config['Main']['logdir']
    loglevel = config['Main']['loglevel'].upper()
    computername = platform.node()
    # Define logfileName
    logfile = logdir + "/" + modulename + "_" + computername + ".log"
    # Configure the root logger
    logger = logging.getLogger()
    level = logging.getLevelName(loglevel)
    logger.setLevel(level)
    # Get logfiles of 1M
    maxbytes = 1024 * 1024
    rfh = logging.handlers.RotatingFileHandler(logfile, maxBytes=maxbytes, backupCount=5)
    # Create Formatter for file
    formatter_file = logging.Formatter(fmt='%(asctime)s|%(module)s|%(funcName)s|%(lineno)d|%(levelname)s|%(message)s',
                                       datefmt='%d/%m/%Y|%H:%M:%S')
    # Add Formatter to Rotating File Handler
    rfh.setFormatter(formatter_file)
    # Add Handler to the logger
    logger.addHandler(rfh)
    # Configure Console Handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter_console = logging.Formatter(fmt='%(asctime)s - %(module)s - %(funcName)s - %(lineno)d - %(levelname)s -'
                                              ' %(message)s',
                                          datefmt='%H:%M:%S')
    # Add Formatter to Console Handler
    ch.setFormatter(formatter_console)
    # logger.addHandler(ch)
    """
    for key in logging.Logger.manager.loggerDict.keys():
        print("Key: {key}".format(key=key))
    """
    logging.getLogger('neo4j.bolt').setLevel(logging.WARNING)
    logging.getLogger('httpstream').setLevel(logging.WARNING)
    return logger

def cleanstr(val):
    """
    This module return val as a lowercase string without leading or trailing blanks. So integers or floats will be
    converted to string.
    The value must be ascii coded, otherwise False will be returned.
    :param val:
    :return: val as string in lowercase ascii code no leading or trailing characters, or False
    """
    wip = str(val)
    try:
        wip.encode("ascii")
    except UnicodeEncodeError:
        # I only want values as ascii character string.
        return False
    else:
        return wip.strip().lower()

def get_inifile(projectname):
    """
    Read Project configuration ini file in subdirectory properties. Config ini filename is the projectname.
    The ini file is located in the properties module, which is sibling of the lib module.
    :param projectname: Name of the project.
    :return: Object reference to the inifile.
    """
    # Use Project Name as ini file.
    # TODO: review procedure to get directory name instead of relative properties/ path.
    if getattr(sys, 'frozen', False):
        # Running Frozen (pyinstaller executable)
        configfile = projectname + ".ini"
    else:
        # Running Live
        # properties directory is sibling of lib directory.
        (filepath_lib, _) = os.path.split(__file__)
        (filepath, _) = os.path.split(filepath_lib)
        # configfile = filepath + "/properties/" + projectname + ".ini"
        configfile = os.path.join(filepath, 'properties', "{p}.ini".format(p=projectname))
    ini_config = configparser.ConfigParser()
    try:
        f = open(configfile)
        ini_config.read_file(f)
        f.close()
    except:
        e = sys.exc_info()[1]
        ec = sys.exc_info()[0]
        log_msg = "Read Inifile not successful: %s (%s)"
        print(log_msg % (e, ec))
        sys.exit(1)
    return ini_config


def get_named_row(nr_name, col_hrd):
    """
    This method will create a named tuple row.
    :param nr_name: Name of the Named Row. Worksheet name in many cases. This will be helpful in case of errors.
    :param col_hrd: Where the column information is stored.
    :return: namedtuple class with name "named_row"
    """
    # Get column names
    field_list = [cell.value for cell in col_hrd]
    # Create named tuple subclass with name "named_row"
    named_row = namedtuple(nr_name, field_list, rename=True)
    return named_row


def neo4j_load_param(filetype, arglist, filedir):
    """
    This function helps to create the arguments for "neo4jadmin load" command. Filedir has all files that have been
    prepared. Filenames are in format "node_label_type.csv" or "rel_label_type.csv". Node and rel are fixed. Label
    relates to the node label, or some default value for relations. type must be alphanumeric, but it is required that
    on a sorted filelist file 00 will be the first file for the node or relation type. The 00 file will have the
    header information.
    :param filetype: Type of the file, the part before first underscore. "node" or "rel". For "node" filetype, argument
    name will be "nodes", for "rel" type argument type will be "relationships".
    :param arglist: Argument list. Entries in the list have the format --nodes=file1,file2,... or --relationships=file1,
    file2,...
    :param filedir: Directory where the files are located.
    :return: argument list is updated with new elements as discovered in the function.
    """
    files = os.listdir(filedir)
    if filetype == "node":
        argname = "--nodes="
    elif filetype == "rel":
        argname = "--relationships="
    else:
        logging.critical("Unknown filetype {ft} see, don't know what to do...".format(ft=filetype))
        sys.exit(1)
    filetype = "{ft}_".format(ft=filetype)
    files4type = [file for file in files if file[:len(filetype)] == filetype]
    files4type.sort()
    # Find first node type
    pos = files4type[0].find("_", len(filetype)+1)
    file_label = files4type[0][:pos]
    file_label_list = []
    for file in files4type:
        # Is this another file for same file type and label?
        if file[:len(file_label)] == file_label:
            file_label_list.append(os.path.join(filedir, file))
        else:
            # Another label found - Remember information for previous label.
            arg = argname + ",".join(file_label_list)
            arglist.append(arg)
            # Initialize this new node type
            pos = file.find("_", len(filetype)+1)
            file_label = file[:pos]
            file_label_list = [os.path.join(filedir, file)]
    # Remember to add last node type list to the set of arguments.
    arg = argname + ",".join(file_label_list)
    arglist.append(arg)
    return


def page_query(q, recinset=1000):
    """
    This function will run a query on a large result set. The query result will be split in pages.
    :param q: Query to run
    :param recinset: Number of records in the result set.
    :return: Yield the result line in iterator format.
    """
    offset = 0
    while True:
        r = False
        for elem in q.limit(recinset).offset(offset):
            r = True
            yield elem
        offset += recinset
        if not r:
            break


def run_script(path, script_name, *args):
    """
    This function will run a python script with arguments.
    :param path: Full path to the script.
    :param script_name: Name of the script. Include .py if this is the script extension.
    :param args: List of script arguments.
    :return:
    """
    script_path = os.path.join(path, script_name)
    cmd = [sys.executable, script_path] + list(args)
    # logging.info(cmd)
    subprocess.call(cmd, env=os.environ.copy())
    return


class LoopInfo:
    """
    This class handles a FOR loop information handling.
    """

    def __init__(self, attribname, triggercnt):
        """
        Initialization of FOR loop information handling. Start message is printed for attribname. Information progress
        message will be printed for every triggercnt iterations.
        :param attribname:
        :param triggercnt:
        :return:
        """
        self.rec_cnt = 0
        self.loop_cnt = 0
        self.attribname = attribname
        self.triggercnt = triggercnt
        curr_time = datetime.now().strftime("%H:%M:%S")
        print("{0} - Start working on {1}".format(curr_time, str(self.attribname)))
        return

    def info_loop(self):
        """
        Check number of iterations. Print message if number of iterations greater or equal than triggercnt.
        :return: Count
        """
        self.rec_cnt += 1
        self.loop_cnt += 1
        if self.loop_cnt >= self.triggercnt:
            curr_time = datetime.now().strftime("%H:%M:%S")
            print("{0} - {1} {2} handled".format(curr_time, str(self.rec_cnt), str(self.attribname)))
            self.loop_cnt = 0
        return self.rec_cnt

    def end_loop(self):
        curr_time = datetime.now().strftime("%H:%M:%S")
        print("{0} - {1} {2} handled - End.\n".format(curr_time, str(self.rec_cnt), str(self.attribname)))
        return self.rec_cnt