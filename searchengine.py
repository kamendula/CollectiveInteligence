__author__ = 'kaka'
# -*- coding: utf-8 -*-

import urllib2
#from BeautifulSoup import *
from bs4 import BeautifulSoup
from urlparse import urljoin
from pysqlite2 import dbapi2 as sqlite
import re


ignorewords = set(['the', 'of','to','and','a','in','is','it'])

class crawler:
    def __init__(self,dbname):
        self.con = sqlite.connect(dbname)

    def __del__(self):
        self.con.close()

    def dbcommit(self):
        self.con.commit()

    def getentryid(self, table, field, value, createnew = True):
        return None

    def addToIndex(self,url,soup):
        if self.isindexed(url): return
        print 'Indexing ' + url

        #获取每个单词
        text = self.gettextonly(soup)
        words = self.separatewords(text)

        #得到url的id
        urlid = self.getentryid('urllist', 'url', url)

        #将每个单词与该url关联
        for i in range(len(words)):
            word = words[i]
            if word in ignorewords: continue
            wordid = self.getentryid('wordlist', 'word', word)
            self.con.execute("insert into wordlocation(urlid, wordid, location)\
                             values (%d,%d,%d)" %(urlid, wordid,i))

    def getentryid(self,table,field,value,createnew=True):
        cur = self.con.execute("select rowid from %s where %s= '%s'" %(table, field, value))
        res = cur.fetchone()

        if res == None:
            cur = self.con.execute("insert into %s (%s) values ('%s')" %(table, field, value))
           # print cur.lastrowid
            return cur.lastrowid
        else:
            return res[0]


    def gettextonly(self, soup):
        v = soup.string
        if v == None:
            c = soup.contents
            resulttext=''
            for t in c:
                subtext = self.gettextonly(t)
                resulttext += subtext + '\n'
            return resulttext
        else:
            return v.strip()

    def separatewords(self,text):
        splitter = re.compile('\\W*')
        return [s.lower() for s in splitter.split(text) if s != '']

    def isindexed(self, url):
        u= self.con.execute("select rowid from urllist where url = '%s'" %url).fetchone()
        if u!= None:
            v = self.con.execute("select * from wordlocation where urlid = %d" % u[0]).fetchone()
            if v != None: return True
        return False

    def addlinkref(self, urlFrom, urlTo, linkText):
        pass

    def createindextables(self):
        self.con.execute('create table urllist(url)')
        self.con.execute('create table wordlist(word)')
        self.con.execute('create table wordlocation(urlid, wordid, location)')
        self.con.execute('create table link(fromid integer, toid integer)')
        self.con.execute('create table linkwords(wordid, linkid)')
        self.con.execute('create index wordids on wordlist(word)')
        self.con.execute('create index urlidx on urllist(url)')
        self.con.execute('create index wordurlidx on wordlocation(wordid)')
        self.con.execute('create index urltoidx on link(toid)')
        self.con.execute('create index urlfromidx on link(fromid)')
        self.dbcommit()


    def crawl(self, pages, depth = 2):
        for i in range(depth):
            newpages = set()
            for page in pages:
                try:
                    c = urllib2.urlopen(page)
                except:
                    print "Could not open %s" %page
                    continue
                print "indexing " + page
                soup = BeautifulSoup(c.read())
                self.addToIndex(page,soup)

                links = soup('a')
                for link in links:
                    if 'href' in dict(link.attrs):
                        url = urljoin(page,link['href'])
                        if url.find("'") != -1:continue
                        url = url.split('#')[0]
                        if url[0:4] == 'http' and not self.isindexed(url):
                            newpages.add(url)
                        linkText = self.gettextonly(link)
                        self.addlinkref(page, url, linkText)

                self.dbcommit()
            pages = newpages


#爬数据
#pagelist = ['http://www.dmoz.org/']
#crawler = crawler('')
#crawler.crawl(pagelist)

#创建数据库
#crawler = crawler('searchindex2.db')
#crawler.__del__()
#crawler.createindextables()

#爬数据并存入到数据库中
#crawler = crawler('searchindex2.db')
#crawler.__del__()
#pagelist = ['http://www.dmoz.org/']
#crawler.crawl(pagelist)
#crawler.con.execute("delete from wordlist ")
#crawler.con.execute("delete from urllist ")
#crawler.con.execute("delete from wordlocation")
#crawler.con.execute("delete from link ")
#crawler.con.execute("delete from linkwords ")

# [ crawler.con.execute("delete from wordlist ")]


class searcher:
    def __init__(self, dbname):
        self.con = sqlite.connect(dbname)

    def __del__(self):
        self.con.close()


    def getmatchrows(self,q):
        #构造查询字符串
        fieldlist = 'w0.urlid'
        tablelist = ''
        clauselist = ''
        wordids = []

        #根据空格拆分单词
        words = q.split(' ')
        tablenumber = 0
        print words

        for word in words:
            #获取单词ID
            #print [row for row in self.con.execute("select word from wordlist")]


            wordrow = self.con.execute("select rowid from wordlist where word = '%s'" % word).fetchone()
            #print wordrow
            if wordrow != None:
                wordid = wordrow[0]
                wordids.append(wordid)
                if tablenumber > 0:
                    tablelist += ','
                    clauselist += ' and '
                    clauselist += 'w%d.urlid = w%d.urlid and '%(tablenumber-1, tablenumber)
                fieldlist += ', w%d.location' %tablenumber
                tablelist += 'wordlocation w%d' % tablenumber
                clauselist += 'w%d.wordid = %d' % (tablenumber, wordid)
                tablenumber += 1


        #根据各个组分，建立查询
        fullquery = 'select %s from %s where %s ' %(fieldlist, tablelist, clauselist)
        #print fullquery
        cur = self.con.execute(fullquery)
        rows = [row for row in cur]

        return rows, wordids


    def getScoredList(self, rows,wordids):
        totalscores = dict([(row[0], 0)  for row in rows])

        #稍后放置评价函数的地方
        weights = []
        for (wight,scores) in weights:
            for url in totalscores:
                totalscores[url] += weight * scores[url]
        return totalscores

    def getUrlName(self, id):
        return self.con.execute("select url from urllist where rowid = %d" % id).fetchone()[0]

    def query(self,q):
        rows, wordids = self.getrmatchrows(q)
        scores = self.getscoredlist(rows)


e = searcher('searchindex2.db')
print e.getmatchrows('haszn lati')















