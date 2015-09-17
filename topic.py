# coding=utf-8
import MySQLdb
from bs4 import BeautifulSoup
import re
import time
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
        for i in xrange(1, 7):
            topic_url = 'http://www.zhihu.com/topic/' + link_id + '/top-answers?page=' + str(i)
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
        self.db.commit()

    def find_question_by_link(self, topic_url, count_id):
        content = get_content(topic_url, count_id)

        if content == "FAIL":
            return 0

        soup = BeautifulSoup(content, 'lxml')

        question_links = soup.findAll('a', attrs={'class': 'question_link'})

        rowcount = 0
        for question_link in question_links:
            # get question id and name
            question_link = question_link.get('href')
            if question_link:
                question_url = 'http://www.zhihu.com' + question_link
                rowcount += self.find_answers_by_question_url(question_url, count_id)
        return rowcount

    def find_answers_by_question_url(self, question_url, count_id):
        content = get_content(question_url, count_id)
        if content == 'Fail':
            return 0
        content = BeautifulSoup(content, 'lxml')

        question_name = content.find(name='div', id='zh-question-title')
        if not question_name:
            return 0

        question_name = question_name.find('h2').get_text().strip()

        answer_num = content.find(name='h3', id='zh-question-answer-num')
        if not answer_num:
            return 0
        answer_num = answer_num.get('data-num')
        get_answer_num = 10

        if 0 < int(answer_num) <= get_answer_num and int(answer_num) > 0:
            get_answer_num = answer_num
        if answer_num <= 0:
            return 0

        question_list = []
        answer_detail_list = []
        time_now = int(time.time())
        question_id = re.sub('.*/', '', question_url)
        question_list = question_list + [(question_name, int(question_id), 0, 0, 0, time_now, 0)]

        answers = content.findAll(name='div', attrs={'class': 'zm-item-answer'}, limit=get_answer_num)
        for answer in answers:
            answer_author_info = answer.find(name='h3', attrs={'class': 'zm-item-answer-author-wrap'}).findAll('a')
            if not answer_author_info:
                continue

            answer_author_id = answer_author_info[1].get('href').replace('/people/', '')
            answer_author_name = answer_author_info[1].get_text()

            answer_detail = answer.find(name='div', attrs={'class': ' zm-editable-content clearfix'})
            answer_detail = str(answer_detail).replace('<div class=" zm-editable-content clearfix">','').replace('</div>','').strip()

            if not answer_detail:
                continue
            # append list

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
