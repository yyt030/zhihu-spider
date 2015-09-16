# coding=utf-8
import MySQLdb
from bs4 import BeautifulSoup
import json
import re
import time
from math import ceil
import logging
import threading
import Queue
import ConfigParser

from util import get_content


class UpdateOneTopic(threading.Thread):
    def __init__(self, queue):
        self.queue = queue
        threading.Thread.__init__(self)

        cf = ConfigParser.ConfigParser()
        cf.read("config.ini")

        host = cf.get("db", "host")
        port = int(cf.get("db", "port"))
        user = cf.get("db", "user")
        passwd = cf.get("db", "passwd")
        db_name = cf.get("db", "db")
        charset = cf.get("db", "charset")
        use_unicode = cf.get("db", "use_unicode")

        self.db = MySQLdb.connect(host=host, port=port, user=user, passwd=passwd, db=db_name, charset=charset,
                                  use_unicode=use_unicode)
        self.cursor = self.db.cursor()

    def run(self):
        while not self.queue.empty():
            t = self.queue.get()
            link_id = t[0]
            count_id = t[1]
            self.find_new_question_by_topic(link_id, count_id)

    def find_new_question_by_topic(self, link_id, count_id):
        new_question_amount_total = 0
        for i in range(1, 7):
            topic_url = 'http://www.zhihu.com/topic/' + link_id + '/questions?page=' + str(i)
            new_question_amount_one_page = self.find_question_by_link(topic_url, count_id)
            new_question_amount_total = new_question_amount_total + new_question_amount_one_page

            if new_question_amount_one_page <= 2:
                break

        if count_id % 2 == 0:
            print str(count_id) + " , " + self.getName() + " Finshed TOPIC " + link_id + ", page " + str(
                i) + " ; Add " + str(new_question_amount_total) + " questions."

        time_now = int(time.time())
        sql = "UPDATE topic SET LAST_VISIT = %s WHERE LINK_ID = %s"
        self.cursor.execute(sql, (time_now, link_id))

    def find_question_by_link(self, topic_url, count_id):
        content = get_content(topic_url, count_id)

        if content == "FAIL":
            return 0

        soup = BeautifulSoup(content, 'lxml')

        question_answers = soup.findAll('div', attrs={'class': 'content'})

        question_list = []
        answer_detail_list = []
        time_now = int(time.time())

        for question_answer in question_answers:
            # get question id and name
            question = question_answer.find(name='a', attrs={'class': 'question_link'})
            if question:
                question_id = question.get('href').replace('/question/', '')
                question_name = re.sub('[\n ]', '', question.get_text())

                question_list = question_list + [(question_name, int(question_id), 0, 0, 0, time_now, 0)]

            # get answer detail and author
            answer_author = question_answer.find(name='h3', attrs={'class': 'zm-item-answer-author-wrap'})
            if answer_author:
                try:
                    answer_author_id = answer_author.find(name='a')
                    answer_author_id = answer_author_id.get('href').replace('/people/', '')
                    answer_author_name = answer_author.find(name='a').get_text()

                    answer_detail = question_answer.find(name='textarea', attrs={'class': 'content hidden'})
                    answer_detail = answer_detail.get_text()
                except AttributeError as e:
                    print e.args, e.message
                    continue

                answer_detail_list = answer_detail_list + [
                    (answer_author_id, answer_author_name, int(question_id), answer_detail)]

        # insert data to DB
        question_sql = 'insert ignore into question (name, link_id, focus, answer, last_visit, add_time, top_answer_number) ' \
                       'values (%s, %s, %s, %s, %s, %s, %s)'
        question__detail_sql = 'insert ignore into answer_detail (answer_author_id, answer_author_name, question_link_id,answer_detail)' \
                               ' values (%s, %s, %s, %s)'
        self.cursor.executemany(question_sql, question_list)
        self.cursor.executemany(question__detail_sql, answer_detail_list)
        self.db.commit()

        return self.cursor.rowcount


class UpdateTopics:
    def __init__(self):
        cf = ConfigParser.ConfigParser()
        cf.read("config.ini")

        host = cf.get("db", "host")
        port = int(cf.get("db", "port"))
        user = cf.get("db", "user")
        passwd = cf.get("db", "passwd")
        db_name = cf.get("db", "db")
        charset = cf.get("db", "charset")
        use_unicode = cf.get("db", "use_unicode")

        self.topic_thread_amount = int(cf.get("topic_thread_amount", "topic_thread_amount"))

        self.db = MySQLdb.connect(host=host, port=port, user=user, passwd=passwd, db=db_name, charset=charset,
                                  use_unicode=use_unicode)
        self.cursor = self.db.cursor()

    def run(self):
        time_now = int(time.time())
        before_last_vist_time = time_now - 10

        queue = Queue.Queue()
        threads = []

        i = 0

        sql = "select link_id from topic where last_visit < %s order by last_visit"
        self.cursor.execute(sql, (before_last_vist_time,))
        results = self.cursor.fetchall()

        for row in results:
            link_id = str(row[0])

            queue.put([link_id, i])
            i = i + 1

        for i in range(self.topic_thread_amount):
            threads.append(UpdateOneTopic(queue))

        for i in range(self.topic_thread_amount):
            threads[i].start()

        for i in range(self.topic_thread_amount):
            threads[i].join()

        self.db.close()

        print 'All task done'


if __name__ == '__main__':
    topic_spider = UpdateTopics()
    topic_spider.run()
