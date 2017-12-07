from collections import OrderedDict
import os
import csv
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

    def all_publicatons(self):
        ''' Select all from publications '''
        dd(self.query("SELECT kind,publicationDate,parent,firstSystemIdentifier,title FROM publications"))
# }}}

class Swiatowid():
    def __init__(self):# {{{
        self.json=Json()
        self._sqlite_init()
        institution="wibp.json"
        self.authors=OrderedDict()
        self.authors_publications=[]
        self._json2sql(institution)

# }}}
    def _sqlite_init(self):# {{{
        try:
            os.remove("swiatowid.sqlite")
        except: 
            pass
        self.s=Sqlite("swiatowid.sqlite")

        self.journals_columns=['issn', 'journal', 'points' ]
        self.s.query("CREATE TABLE journals({})".format(",".join(self.journals_columns)))

        self.publications_columns=['firstSystemIdentifier', 'title', 'kind', 'publicationDate', 'parent' ]
        self.s.query("CREATE TABLE publications({})".format(",".join(self.publications_columns)))

        self.authors_columns=['pbnId', 'familyName', 'givenNames', 'affiliatedToUnit', 'employedInUnit' ]
        self.s.query("CREATE TABLE authors({})".format(",".join(self.publications_columns)))

        self.authors_publications=['pbnId_author', 'firstSystemIdentifier_article' ]
        self.s.query("CREATE TABLE authors_publications({})".format(",".join(self.authors_publications)))
# }}}
    def _get_issn(self,work):# {{{
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
    def _make_publications(self,a):# {{{
        if a['kind']=='Article':
            parent=a['journal']['issn'].strip()

        elif a['kind']=='Book':
            parent=a['isbn'].strip()

        elif a['kind']=='Chapter':
            parent=a['book']['isbn'].strip()

        return (a['firstSystemIdentifier'] , a['title'] , a['kind'] , a['publicationDate'] , parent) 

# }}}
    def _make_authors(self,a):# {{{
        if a['affiliatedToUnit']==True:
            a['affiliatedToUnit']=1
        else:
            a['affiliatedToUnit']=0
        if a['employedInUnit']==True:
            a['employedInUnit']=1
        else:
            a['employedInUnit']=0

        return (a['pbnId'], a['familyName'], a['givenNames'], a['affiliatedToUnit'], a['employedInUnit'])
# }}}
    def _make_authors_publications(self,a):# {{{
        pass
# }}}
    def _json2sql(self, institution):# {{{
        publications=[]
        authors=[]
        for a in self.json.read(institution):
            publications.append(self._make_publications(a))
            for author in a['authors']:
                authors.append(self._make_authors(author))
            #self._make_authors_publications(a)

        authors=list(set(authors))

        self.s.executemany('INSERT INTO publications VALUES (?,?,?,?,?)', publications)
        self.s.executemany('INSERT INTO authors VALUES (?,?,?,?,?)', authors)
        self.s.all_publicatons()

# }}}
    def journals_csv2sql(self):# {{{
        with open('journals.csv', 'r') as csvfile:
            reader = csv.reader(csvfile, delimiter=';')
            data=[]
            for i in reader:
                data.append([ i[0], int(i[1]), i[2] ])
        self.s.executemany('INSERT INTO journals VALUES (?,?,?)', data)
        #dd(self.s.query("select * from journals"))
# }}}

z=Swiatowid()
z.journals_csv2sql()
