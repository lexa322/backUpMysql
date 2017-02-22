#!/usr/bin/python

import MySQLdb
import subprocess
import time
import email
import email.mime.application
import smtplib
import mimetypes
import socket
import os
import logging
from email.MIMEText import MIMEText
from datetime import datetime


backUpDIR = "/var/tmp/"
bindir = "/usr/bin"
userBD = "user"
passwBD = "xxxxxx"
hostBD = "localhost"
logfile = "/var/log/mysql/backup-mysql.log"
nice = "/usr/bin/nice -n 10"
exclude = ["performance_schema", "information_schema"]
host_name = socket.gethostname()

logging.basicConfig(format = '%(filename)s - %(levelname)-3s [%(asctime)s] %(message)s ', filename=logfile, level=logging.DEBUG)

def mysql_connect (hostBD, userBD, passwBD):
    database_list = []
    conn = MySQLdb.connect(hostBD, userBD, passwBD, charset="utf8")
    cursor=conn.cursor()
    return(cursor)
 
def select_bd (cursor):
    database_list = []
    cursor.execute("show databases")
    for line in cursor.fetchall():
        database_list.append(line[0])
    return(database_list)
    cursor.close()

def create_dir (back_dir):
    dirname = os.path.join(backUpDIR, back_dir, "database/")
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    return(dirname)


def send_mail ():
    fromaddr = "xxx@mail.ru"
    toaddr = "yyy@mail.ru"
    serv = "smtp_serv" 
    port = 25
    msg = MIMEText("Backup completed successfully!")
    msg["Subject"] = "Backup database in " + host_name
    msg["From"] = fromaddr
    msg["To"] = toaddr
    s = smtplib.SMTP(serv, port) 
    s.login("user", "password")
    s.sendmail(fromaddr, toaddr, msg.as_string())
    s.quit()
  
def mysql_dump (userBD, passwBD, database, archname):
    subprocess.call("mysqldump -u %s -p%s --single-transaction %s | gzip > %s" % (userBD, passwBD, database, archname), shell=True)

def run_mysqldump ():
    cursor = mysql_connect(hostBD, userBD, passwBD)
    database = select_bd(cursor)
    backUpDate = datetime.strftime(datetime.now(), "%Y.%m.%d")
    for database in  database:
        if database not in exclude:
            backup_dir = create_dir(database)
            if backup_dir:
                archname = (backup_dir + database + backUpDate + '.sql.gz')
                if not os.path.isfile(archname):
                    mysql_dump (userBD, passwBD, database, archname)
                else:
                    logging.info ("Backup %s already present, skip" % archname)
    logging.info ("Database %s in exclude list, skip" % exclude)
    send_mail()
     

if __name__ == '__main__':
    starttime = time.strftime('%Y-%m-%d %H:%M:%S')
    logging.info('Backup started at: %s' % starttime)
    run_mysqldump()
    finishtime = time.strftime('%Y-%m-%d %H:%M:%S')
    logging.info('Backup finished at: %s' % finishtime)
