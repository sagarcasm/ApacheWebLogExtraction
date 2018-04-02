import pymysql

class webLogs:

    def __init__(self, path):
        self.path = path

    def initialdataLoad(self):
        print(self.path)
        data = []
        #read lines from the csv
        with open(self.path) as file:
            file = file.readlines()

        for line in file:
            print(line)
            logdata = line.split(" ")

            ip = logdata[0]
            dateTime = self.cleanData(logdata[3] + " " + logdata[4])
            methods = self.cleanData(logdata[5])
            requestResource = self.cleanData(logdata[6])
            protocol = self.cleanData(logdata[7])
            status = self.cleanData(logdata[8])
            bytesTransfered = self.cleanData(logdata[9])

            try:
                requestUrl = logdata[10]
            except IndexError:
                requestUrl =  "NA"

            if (bytesTransfered == '-'):
                bytesTransfered = '0'

            http_user_agent = " ".join(logdata[11:])
            if (http_user_agent == ''):
                http_user_agent = 'NA'


            logDataDict = {'remote_addr':ip,'time_local':dateTime,'request_type':methods,'request_resource':requestResource,'request_url':requestUrl,\
                           'request_status': status,'bytes_sent':bytesTransfered,'http_referer':protocol,'http_user_agent':http_user_agent,'raw_log':line,}

            self.insertQuery(logDataDict)


    # function to insert data into database
    def insertQuery(self,data):

        # database paramters
        HOSTNAME = '----ADD ypur MYSQL host-----'
        USER = '----ADD ypur MYSQL USER-----'
        PASSWORD = '----ADD ypur MYSQL PASSKEY-----'
        DATABASE = '----ADD ypur MYSQL DB-----'

        sqlConnection = pymysql.connect(host=HOSTNAME, user=USER, passwd=PASSWORD, db=DATABASE)
        cur = sqlConnection.cursor()

        query =  "INSERT INTO weblogs (remote_addr, time_local, request_type,request_resource, request_url,request_status,bytes_sent, http_referer, http_user_agent,raw_log) \
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

        try:
            cur.execute(query, (
            str(data['remote_addr']), str(data['time_local']), str(data['request_type']), str(data['request_resource']),
            str(data['request_url']), \
            str(data['request_status']), str(data['bytes_sent']), str(data['http_referer']),
            str(data['http_user_agent']), str(data['raw_log'])))
            sqlConnection.commit()

            #for debug purposes to check your Query
            # if cur.lastrowid:
            #     print('last insert id', cur.lastrowid)
            #     print(cur._last_executed)
            # else:
            #     print('last insert id not found')

        except:
            sqlConnection.rollback()

        sqlConnection.close()

    #clean the data to strip of the extra special characters from the data
    def cleanData(self,item):
        characters = ['[', ']', "'", '\n']
        for char in characters:
            item = item.replace(char, "")
        return item

#provide the directory of Apache weblog file you want to upload in the SQL table
wl = webLogs("ADD your web log location")
wl.initialdataLoad()

