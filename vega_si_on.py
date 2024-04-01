import configparser
import websocket
import json
import time
import psycopg2
import logging
from logging.handlers import RotatingFileHandler


config = configparser.ConfigParser()
config.read('conf.ini')

api_url = config.get('server', 'server_url')
api_user = config.get('server', 'server_user')
api_password = config.get('server', 'server_password')

db_host = config.get('db', 'db_host')
db_dbname = config.get('db', 'db_name')
db_user = config.get('db', 'db_user')
db_password = config.get('db', 'db_password')

path_log = config.get('log', 'log_path')

template_data = "030X00"
time_sleep = 2


if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = logging.handlers.RotatingFileHandler(path_log, maxBytes=5000000, backupCount=5)
    # handler = logging.FileHandler(path_log, mode='a')
    # logging.handlers.RotatingFileHandler(filename, maxBytes, backupCount)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logger.info(f'log:: start')
    # print('log:: start')

    while True:
        start_time = time.monotonic()

        try:
            conn = psycopg2.connect(dbname=db_dbname, user=db_user, password=db_password, host=db_host)
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT da.deveui AS td_id, da.value::json ->> 'info_channel' AS td_link_to_si
                FROM deviceattributes AS da
                WHERE da.attribute = 'other_info_1' AND da.value::json ->> 'info_channel' ~ '.*-1|2';
                """
            )
            records = cursor.fetchall()

            for record in records:
                td_id = record[0]
                td_link_to_si = record[1]
                si_id = td_link_to_si.split('-')[0]
                si_port = td_link_to_si.split('-')[1]

                cursor.execute(
                    f"""
                    SELECT gateway.active
                    FROM coveragemap AS cm
                    LEFT JOIN bs AS gateway ON cm.macbs = gateway.mac
                    WHERE cm.deveui = '{si_id}';
                    """
                )
                records = cursor.fetchone()

                if records is not None:
                    gateway_status = records[0]
                    # print(gateway_status)

                    if gateway_status != 0:
                        cursor.execute(
                            f"""
                            SELECT MAX(rd.time) AS last_data_td_alarm
                            FROM rawdata AS rd
                            WHERE rd.deveui = '{td_id}'
                            AND SUBSTRING(encode(rd.data, 'hex'),1,2) = '01'
                            AND SUBSTRING(encode(rd.data, 'hex'),23,2) = '05';
                            """
                        )
                        records = cursor.fetchone()

                        if records[0] is not None:
                            last_data_td_alarm_ut = records[0]

                            cursor.execute(
                                f"""
                                SELECT rd.time AS first_data_si_off
                                FROM rawdata AS rd
                                WHERE rd.deveui = '{si_id}'  -- si_id
                                AND rd.port = 2
                                AND SUBSTRING(encode(rd.data, 'hex'),1,2) = '05'
                                AND SUBSTRING(encode(rd.data, 'hex'),8,1) = '{si_port}'  -- si_port
                                AND SUBSTRING(encode(rd.data, 'hex'),10,1) = '1'
                                AND time >= {last_data_td_alarm_ut}  -- last_data_td_alarm_ut
                                ORDER BY rd.time ASC LIMIT 1;
                                """
                            )
                            records = cursor.fetchone()
                            if records is not None:
                                first_data_si_off_ut = records[0]
                                # logger.info(f'log:: {td_id} >> {si_id} >> {si_port} >> {last_data_td_alarm_ut} >> {first_data_si_off_ut}')
                                # print(f'log:: {td_id} >> {si_id} >> {si_port} >> {last_data_td_alarm_ut} >> {first_data_si_off_ut}')
                            else:
                                try:
                                    # API Connection
                                    ws = websocket.WebSocket()
                                    ws.connect(api_url)

                                    # API Authentication
                                    message_auth_req = {"cmd": "auth_req", "login": api_user, "password": api_password}
                                    ws.send(json.dumps(message_auth_req))
                                    api_res_auth = json.loads(ws.recv())
                                    api_auth_token = api_res_auth["token"]
                                    api_auth_status = api_res_auth["status"]

                                    if api_auth_status is True:
                                        # API send data SI switching ON
                                        data = template_data.replace('X', si_port)

                                        message_send_data_req = {"cmd": "send_data_req", "data_list": [{"devEui": si_id, "data": data, "port": 2, "ack": True}]}
                                        ws.send(json.dumps(message_send_data_req))
                                        api_res_send_data = json.loads(ws.recv())
                                        api_send_data_status = api_res_send_data["status"]
                                        logger.info(f'log:: {td_id} >> {si_id} >> {si_port} >> {last_data_td_alarm_ut} >> None >> api_send_data_status: {api_send_data_status}')
                                        # print(f'log:: {td_id} >> {si_id} >> {si_port} >> {last_data_td_alarm_ut} >> None >> api_send_data_status: {api_send_data_status}')

                                        # API login out
                                        message_close_auth = {"cmd": "close_auth_req", "token": api_auth_token}
                                        ws.send(json.dumps(message_close_auth))
                                        ws.recv()

                                        # API disconnected
                                        ws.close()

                                    else:
                                        logger.warning(f'log:: {td_id} >> {si_id} >> {si_port} >> {last_data_td_alarm_ut} >> None >> api_auth: False')
                                        # print(f'log:: {td_id} >> {si_id} >> {si_port} >> {last_data_td_alarm_ut} >> None >> api_auth: False')

                                except ConnectionRefusedError:
                                    logger.warning(f'log:: {td_id} >> {si_id} >> {si_port} >> {last_data_td_alarm_ut} >> None >> api_conn: False')
                                    # print(f'log:: {td_id} >> {si_id} >> {si_port} >> {last_data_td_alarm_ut} >> None >> api_conn: False')

                        # else:
                            # logger.info(f'log:: {td_id} >> {si_id} >> {si_port} >> None')
                            # print(f'log:: {td_id} >> {si_id} >> {si_port} >> None')
                    else:
                        logger.warning(f'log:: {td_id} >> {si_id} >> {si_port} >> gateway_status: False')

            cursor.close()
            conn.close()

        except psycopg2.Error:
            logger.warning('log:: db: False')
            # print('log:: db: False')

        time.sleep(time_sleep)

        scr_time = time.monotonic() - start_time
        if scr_time > 4:
            logger.warning(f'log:: scr_time: {scr_time}')
