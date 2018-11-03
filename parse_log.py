"""
This script will parse the apache log file and convert each line in an action record and a user agent record. The script
will process logfile other_vhosts_access.log.1. Each time a change in the file is detected the assumption is that this
is a new file and every line in the file is processed.
"""

import apache_log_parser
import hashlib
import logging
import time
from lib import my_env
from lib import sqlstore
from lib.sqlstore import Request, UserAgent, FileHash
from sqlalchemy.orm.exc import NoResultFound


def file_update(file_id, fc):
    """
    This method will check if the file is updated since last run.
    :param file_id: Unique identifier for the file, use filename.
    :param fc: Contents (slurp) of the file.
    :return: True if the file was updated, False if there is no change.
    """
    # Get the hash for the file
    m = hashlib.sha256()
    m.update(fc)
    fh = m.hexdigest()
    # Check if file is changed since last run
    try:
        fr = sess.query(FileHash).filter_by(file_id=file_id).one()
    except NoResultFound:
        # New file, add to database
        frec = FileHash(
            file_id=file_id,
            fh=fh,
            created=int(time.time()),
            modified=int(time.time())
        )
        sess.add(frec)
        sess.commit()
        return True
    # Existing file, check if there was an update
    if fr.fh == fh:
        # Same checksum, no need to update
        return False
    else:
        fr.fh = fh
        fr.modified = int(time.time())
        sess.commit()
        return True


cfg = my_env.init_env("visitors", __file__)
logging.info("Start application")
sess = sqlstore.init_session(cfg["Main"]["db"])
logfile = cfg["Main"]["apache_log"]
# Slurp logfile to calculate checksum.
with open(logfile, 'rb') as fp:
    if not file_update("other_vhosts", fp.read()):
        raise SystemExit(0)

# logfile other_vhosts_access_log.1 has been updated, load all records in database.
res = sess.query(UserAgent).all()
ua_dict = {}
for rec in res:
    ua_dict[rec.desc] = rec.id
parse_str = "%v:%p %h %l %u %t \"%r\" %>s %O \"%{Referer}i\" \"%{User-Agent}i\""
line_parser = apache_log_parser.make_parser(parse_str)
cnt = 0
# Now open logfile and handle line by line.
with open(logfile, 'r') as file:
    line = file.readline()
    while line:
        line_data = line_parser(line)
        desc = line_data["request_header_user_agent"]
        if desc not in ua_dict:
            ua = UserAgent(
                desc=line_data["request_header_user_agent"],
                browser_family=line_data["request_header_user_agent__browser__family"],
                browser_version=line_data["request_header_user_agent__browser__version_string"],
                os_family=line_data["request_header_user_agent__os__family"],
                os_version=line_data["request_header_user_agent__os__version_string"],
                mobile=line_data["request_header_user_agent__is_mobile"]*1
            )
            sess.add(ua)
            sess.commit()
            sess.refresh(ua)
            ua_dict[ua.desc] = ua.id
        request = Request(
            hostip=line_data["remote_host"],
            version=line_data["request_http_ver"],
            url=line_data["request_url"],
            server=line_data["server_name"],
            port=line_data["server_port"],
            status=line_data["status"],
            bytes=line_data["bytes_tx"],
            referer=line_data["request_header_referer"],
            timestamp=line_data["time_received_isoformat"],
            uagent_id=ua_dict[desc]
        )
        sess.add(request)
        sess.commit()
        line = file.readline()
