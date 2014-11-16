'''
Created on Nov 13, 2014

@author: arunjacob
'''
import MySQLdb
import json
import pika
import threading
import logging
import datetime

logging.basicConfig()

class MessageEncoder(json.JSONEncoder):
        
    def default(self,o):
        return o.__dict__
    
messageLogger = logging.getLogger('message')
messageLogger.setLevel(logging.DEBUG)

class Message(object):
    '''
    classdocs
    '''


    def __init__(self, row = None, details = None):
        


        if row != None:
            messageLogger.debug("initializing from database")
            
            self.message_id = row[0]
            self.sequence_id = row[1]
            self.sequence_value = row[2]
            self.created_date = row[3]
        elif details != None:
            messageLogger.debug("initializing from JSON")
            self.message_id = -1
            if details.has_key('sequence_id') == True:
                self.sequence_id = details['sequence_id']
            else:
                messageLogger.error("invalid JSON format, sequence_id not found")
                raise 'invalid format'
            
            if details.has_key('sequence_value') == True:
                self.sequence_value = details['sequence_value']
            else:
                messageLogger.error("invalid JSON format, sequence_value  not found")
                raise 'invalid format'    
            
            # created is optional. It's always overwritten on insert to db.
            if details.has_key('created_date'):
                self.created_date = details['created_date']
    
    
    
class MessageDB(object):
    
    def __init__(self,url,dbName,userName,password):
        
        self.log =  logging.getLogger('messageDB')
        self.log.setLevel(logging.DEBUG)

        try:
            self.log.debug("connecting database")
            self.db = MySQLdb.connect(host=url,user=userName,passwd=password,db=dbName)
            cur = self.db.cursor()
            cur.execute('use %s'%dbName)
        except MySQLdb.Error, e:
            self.log.error("unable to connect to database")
            self.handleMySQLException(e,True)
            
            
            
    def handleMySQLException(self,e,throwEx=False):
        try:
            self.log.error( "Error [%d]: %s"%(e.args[0],e.args[1]))
        except IndexError:
            self.log.error( "Error: %s"%str(e))
            
        raise e
    
    def addMessage(self,message):
        
        try:
            #created_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            created_date = unicode(datetime.datetime.now())
            self.log.debug("adding message into database with sequence_id = %d and sequence_value = %d and created_date = '%s'"%(message.sequence_id,message.sequence_value,created_date))
            query = "insert into messages(sequence_id, sequence_value,created_date) values(%d,%d,'%s')"%(message.sequence_id, message.sequence_value,created_date)
            cur = self.db.cursor()
            cur.execute(query)
            self.db.commit()
                
                
        except MySQLdb.Error as e:
            self.log.error(str(e))
            self.handleMySQLException(e)
       
       
    def getMessages(self,isDescending=True,limit = 100):
        msgs = []
        self.log.debug("retrieving messages, limit = %d"%limit)
        try:
            if isDescending == True:
                query = 'select message_id,sequence_id, sequence_value,created_date from messages order by message_id DESC LIMIT %d'%limit
            else:
                query = 'select message_id,sequence_id, sequence_value,created_date from messages order by message_id LIMIT %d'%limit # will order ASC because message_id is the primary key
            
            cur = self.db.cursor()
            cur.execute(query)
            rows = cur.fetchall()
            
            for row in rows:
                msgs.append(Message(row))
        except MySQLdb.Error, e:
            self.handleMySQLException(e)
    
        self.log.debug("returning %d messages"%len(msgs))
        return msgs
    

    def dropAllMessages(self):
        self.log.debug("dropping all messages")
        try:
            query = "TRUNCATE TABLE messages"
            cur = self.db.cursor()
            cur.execute(query)
            self.db.commit()

        except MySQLdb.Error, e:
            self.log.error(str(e))
            self.handleMySQLException(e)
            
            
class MessageQueue:
    def __init__(self,amqp_url,messageDB):
        self.log =  logging.getLogger('messageQueue')
        self.log.setLevel(logging.DEBUG)
        self.amqp_url = amqp_url
        self.messageDB = messageDB
        self.channel = None
        
    
        
    def getMessages(self, queue_name,messageCount):
        self.log("getting %d messages from queue"%messageCount)
        parameters = pika.URLParameters(self.amqp_url)
        connection = pika.BlockingConnection(parameters)

        channel = connection.channel()
        i = 0
        
        for i in range(0,messageCount):
            
            method_frame, header_frame, body = channel.basic_get(queue_name)
            if method_frame:
                try:
                    self.decodeAndAddMessage(body) 
                except:
                    self.log.error ("invalid format of message, removing message from queue")
                
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                    
            else:
                break
            
    def decodeAndAddMessage(self,messageBody):
        messageContents = json.loads(messageBody)
    
        message = Message(None,messageContents)
        self.messageDB.addMessage(message)
        
                
    def getMessagesAsync(self,queue_name):
        self.log.debug("spawning async message processing thread")
        d = threading.Thread(name='daemon', target=self.asyncMessageConsumption, args = (queue_name,))
        d.setDaemon(True)
        d.start()
        
    def asyncMessageConsumption(self,queue_name):
        self.log.debug("async message processing thread, setting up RabbitMQ Connection")
        parameters = pika.URLParameters(self.amqp_url)
        connection = pika.BlockingConnection(parameters)

        channel = connection.channel()
        try:
            self.log.debug("kicking off channel creation...")
            #I believe queue_declare will only create the queue if doesn't already exist.
            channel.queue_declare(queue=queue_name)
            channel.basic_consume(self.on_message, queue_name)
            self.log.debug("starting to consume messages...")
            channel.start_consuming()
        except Exception, err:
            print Exception, err

        # note that this should probably be part of a cleanup method that gets invoked as part of thread shutdown.
        channel.stop_consuming()
        connection.close()
        self.log.debug("finished consuming messages...")
        
        
    def on_message(self,channel, method_frame, header_frame, body):
        self.log.debug("processing asynchronously received message")
        try:
            self.decodeAndAddMessage(body) 
        except:
            self.log.error ("invalid format of message, removing message from queue")
                
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        
    
        