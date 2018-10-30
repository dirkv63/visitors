"""
This script will parse the apache log file and convert each line in an action record and a user agent record. This
script will handle all new records from the logfile.
"""
import apache_log_parser
import logging
from lib import my_env
from lib import sqlstore
from lib.sqlstore import Request, UserAgent

cfg = my_env.init_env("visitors", __file__)
logging.info("Start application")
sess = sqlstore.init_session(cfg["Main"]["db"])
res = sess.query(UserAgent).all()
ua_dict = {}
for rec in res:
    ua_dict[rec.desc] = rec.id
logfile = "data/other_vhosts_access.log.1"
parse_str = "%v:%p %h %l %u %t \"%r\" %>s %O \"%{Referer}i\" \"%{User-Agent}i\""
line_parser = apache_log_parser.make_parser(parse_str)
cnt = 0
with open(logfile, 'r') as file:
    line = file.readline()
    li = my_env.LoopInfo("logrecords", 100)
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
        try:
            request = Request(
                hostip=line_data["remote_host"],
                version=line_data["request_http_ver"],
                path=line_data["request_url_path"],
                #query=line_data["request_url_query"],
                server=line_data["server_name"],
                port=line_data["server_port"],
                status=line_data["status"],
                bytes=line_data["bytes_tx"],
                uagent_id=ua_dict[desc]
            )
            sess.add(request)
            sess.commit()
            line = file.readline()
        except  KeyError:
            for k in line_data:
                print("{k}: {v}".format(k=k, v=line_data[k]))
            break
        li.info_loop()
    li.end_loop()
