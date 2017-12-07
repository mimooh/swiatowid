from collections import OrderedDict
import os
import sys
import sqlite3
import inspect
import json

class dd():# {{{
    def __init__(self,struct):
        '''debugging function, much like print but handles various types better'''
        print()
        if isinstance(struct, list):
            for i in struct:
                print(i)
        elif isinstance(struct, dict):
            for k, v in struct.items():
                print (str(k)+':', v)
        else:
            print(struct)
# }}}
class Json(): # {{{
    def read(self,path): 
        try:
            f=open(path, 'r')
            dump=json.load(f, object_pairs_hook=OrderedDict)
            f.close()
            return dump
        except:
            raise Exception("\n\nMissing or invalid json: {}.".format(path)) 

    def write(self, data, path, pretty=1): 
        try:
            if pretty==1:
                pretty=json.dumps(data, indent=4)
                f=open(path, 'w')
                f.write(pretty)
                f.close()
            else:
                f=open(path, 'w')
                json.dump(data, f)
                f.close()
        except:
            raise Exception("\n\nCannot write json: {}.".format(path)) 


# }}}
class Sqlite(): # {{{
    def __init__(self, handle):
        self.SQLITE = sqlite3.connect(handle)
        self.SQLITE.row_factory=self._sql_assoc
        self.sqlitedb=self.SQLITE.cursor()

    def _sql_assoc(self,cursor,row):
        ''' Query results returned as dicts. '''
        d = OrderedDict()
        for id, col in enumerate(cursor.description):
            d[col[0]] = row[id]
        return d

    def query(self,query,data=tuple()):
        ''' Query sqlite, return results as dict. '''
        self.sqlitedb.execute(query,data)
        self.SQLITE.commit()
        if query[:6] in("select", "SELECT"):
            return self.sqlitedb.fetchall() 

    def executemany(self,query,data=tuple()):
        ''' Query sqlite, return results as dict. '''
        self.sqlitedb.executemany(query,data)
        self.SQLITE.commit()

    def querydd(self,query,data=tuple()):
        ''' Debug query, instead of connecting shows the exact query and params. '''
        print(query)
        print(data)

    def dump(self):
        print("dump() from caller: {}".format(inspect.stack()[1][3]))
        #for i in self.query('SELECT * FROM todo order by todo'):
        #    print(i)
# }}}

class Swiatowid():
    def __init__(self):# {{{
        self.json=Json()
        self._sqlite_init()
        institution="wibp.json"
        self.articles=[]
        self.authors=OrderedDict()
        self.authors_articles=[]
        self._json2sql(institution)

# }}}
    def _sqlite_init(self):# {{{
        try:
            os.remove("swiatowid.sqlite")
        except: 
            pass
        self.s=Sqlite("swiatowid.sqlite")

        self.articles_columns=['firstSystemIdentifier', 'title', 'publicationDate', 'journal' ]
        self.s.query("CREATE TABLE articles({})".format(",".join(self.articles_columns)))

        self.authors_columns=['pbnId', 'familyName', 'givenNames', 'affiliatedToUnit', 'employedInUnit' ]
        self.s.query("CREATE TABLE authors({})".format(",".join(self.articles_columns)))

        self.authors_articles=['pbnId_author', 'firstSystemIdentifier_article' ]
        self.s.query("CREATE TABLE authors_articles({})".format(",".join(self.authors_articles)))
# }}}
    def _get_issn_isbn(self,work):# {{{
        keys=work.keys()
        if "journal" in keys:
            if "issn" in work["journal"] and len(work["journal"]["issn"])>0:
                return work["journal"]["issn"].strip()

        if "book" in keys:
            if "isbn" in work["book"] and len(work["book"]["isbn"])>0 :
                return work["book"]["isbn"].strip()

        if "issn" in keys and len(work["issn"])>0 :
            return work["issn"].strip()
            
        if "isbn" in keys and len(work["isbn"])>0 :
            return work["isbn"].strip()
            
        return "err"

# }}}
    def _make_articles(self,a):# {{{
        issn_isbn=self._get_issn_isbn(a)
        self.articles.append(OrderedDict([ 
            ('firstSystemIdentifier' , a['firstSystemIdentifier']) ,
            ('title'                 , a['title'])                 ,
            ('publicationDate'       , a['publicationDate'])       ,
            ('journal'               , issn_isbn)
            ])
        )
        dd(self.articles)

# }}}
    def _make_authors(self,a):# {{{
        pass
# }}}
    def _make_authors_articles(self,a):# {{{
        pass
# }}}
    def _json2sql(self, institution):# {{{
        for a in self.json.read(institution):
            self._make_articles(a)
            self._make_authors(a)
            self._make_authors_articles(a)

# }}}

z=Swiatowid()
