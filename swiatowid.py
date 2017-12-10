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
        dd(self.query("SELECT kind,publicationDate,parent,publicationId,title FROM publications"))

    def all_authors(self):
        ''' Select all from authors'''
        dd(self.query("SELECT authorId,familyName,givenNames FROM authors"))

    def all_authors_publications(self):
        ''' Select all from authors_publications '''
        dd(self.query("SELECT * FROM authors_publications"))

    def all_v(self):
        ''' Select all from authors_publications '''
        dd(self.query("SELECT * FROM v"))

# }}}

class Swiatowid():
    def __init__(self):# {{{
        self.json=Json()
        self._sqlite_init()
        institution="wibp.json"
        self.authors=OrderedDict()
        self.authors_publications=[]
        self._json2sql(institution)
        self._journals_csv2sql()
        self._plot_data()
        self._authors_details()

# }}}
    def _sqlite_init(self):# {{{
        try:
            os.remove("swiatowid.sqlite")
        except: 
            pass
        self.s=Sqlite("swiatowid.sqlite")

        self.s.query("CREATE TABLE publications(publicationId, title, kind, year, parent)")
        self.s.query("CREATE TABLE authors(authorId, familyName, givenNames, affiliatedToUnit, employedInUnit )")
        self.s.query("CREATE TABLE authors_publications(author_id, publication_id)")
        self.s.query("CREATE TABLE journals(issn, points, letter, journal)")
        self.s.query("CREATE VIEW v AS SELECT a.familyName, a.givenNames, a.authorId, p.title, p.year, j.letter, j.points, j.journal FROM journals j, authors a , publications p , authors_publications ap WHERE ap.author_id=a.authorId AND ap.publication_id=p.publicationId AND j.issn=p.parent AND p.kind='Article';")


# }}}
    def _plot_data(self):# {{{
        plot_data=self.s.query("select familyName, givenNames, authorId,  sum(points) as points from v  group by familyName order by points DESC ")
        self.json.write(plot_data,"plot_data.json")

#}}}
    def _authors_details(self):# {{{
        authors_details=OrderedDict()
        ids=[]
        for i in self.s.query("SELECT DISTINCT authorId,familyName,givenNames FROM v"):
            authors_details[i['authorId']]=OrderedDict()
            authors_details[i['authorId']]['meta']=(i['familyName'], i['givenNames'])
            authors_details[i['authorId']]['works']=[]
            for j in self.s.query("SELECT letter,year,points,title,journal FROM v WHERE authorId=? ORDER BY year", (i['authorId'],)):
                authors_details[i['authorId']]['works'].append(j)
        self.json.write(authors_details, "authors_details.json")
#}}}

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
    def _publication_record(self,a):# {{{
        if a['kind']=='Article':
            parent=a['journal']['issn'].strip()

        elif a['kind']=='Book':
            parent=a['isbn'].strip()

        elif a['kind']=='Chapter':
            parent=a['book']['isbn'].strip()

        return (a['firstSystemIdentifier'] , a['title'] , a['kind'] , a['publicationDate'] , parent) 

# }}}
    def _author_record(self,a):# {{{
        if a['affiliatedToUnit']==True:
            a['affiliatedToUnit']=1
        else:
            a['affiliatedToUnit']=0
        if a['employedInUnit']==True:
            a['employedInUnit']=1
        else:
            a['employedInUnit']=0

        try:
            return (a['pbnId'], a['familyName'], a['givenNames'], a['affiliatedToUnit'], a['employedInUnit'])
        except:
            raise Exception("Problem with this author", a)
# }}}
    def _json2sql(self, institution):# {{{
        publications=[]
        authors=[]
        authors_publications=[]

        for json_record in self.json.read(institution):
            p=self._publication_record(json_record)
            publications.append(p)
            for author in json_record['authors']:
                a=self._author_record(author)
                authors.append(a) 
                if p[2]=='Article':
                    authors_publications.append((a[0],p[0]))

        self.s.executemany('INSERT INTO publications VALUES (?,?,?,?,?)', publications)
        self.s.executemany('INSERT INTO authors VALUES (?,?,?,?,?)', set(authors))
        self.s.executemany('INSERT INTO authors_publications VALUES (?,?)', set(authors_publications))
        #self.s.all_publicatons()
        #self.s.all_authors()
        #self.s.all_authors_publications()


# }}}
    def _journals_csv2sql(self):# {{{
        with open('journals.csv', 'r') as csvfile:
            reader = csv.reader(csvfile, delimiter=';')
            data=[]
            for i in reader:
                data.append([ i[0], int(i[1]), i[2], i[3] ])
        self.s.executemany('INSERT INTO journals VALUES (?,?,?,?)', data)
        #dd(self.s.query("select * from journals"))
# }}}

z=Swiatowid()
z.s.all_v()
