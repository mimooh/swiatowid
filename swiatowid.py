from collections import OrderedDict
import os
import csv
import argparse
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
        dd(self.query("SELECT * FROM publications"))

    def all_authors(self):
        ''' Select all from authors'''
        dd(self.query("SELECT authorId,familyName,givenNames FROM authors"))

    def all_authors_publications(self):
        ''' Select all from authors_publications '''
        dd(self.query("SELECT * FROM authors_publications"))

    def all_journals(self):
        dd(self.query("SELECT * FROM journals"))

    def all_v(self):
        dd(self.query("SELECT * FROM v"))

# }}}

class Swiatowid():
    def __init__(self):# {{{
        self.s=Sqlite("swiatowid.sqlite")
        self.anonymize=0
        self.json=Json()
        self._argparse()
        self._main()
        self._plot_data()

# }}}
    def _argparse(self):# {{{
        parser = argparse.ArgumentParser(description='Options for swiatowid')

        parser.add_argument('-a' , help="Anonymize authors"                   , required=False , action='store_true')
        parser.add_argument('-p' , help="PBN API's json for your institution" , required=False , type=str             , default='wibp.json')

        args = parser.parse_args()

        if args.a:
            self.anonymize=1
        if args.p:
            self.institution=args.p
# }}}
    def _sqlite_init(self):# {{{
        ''' 
        Try to create the tables. Sqlite will fail if they exist, so we just hide the error messages under try:
        Always delete data from the tables. 
        Don't clear authors_shares, since we are the only db for this data.
        '''

        try:
            self.s.query("CREATE TABLE publications(publicationId, title, kind, year, parentId, parentTitle, authors, pointsShare)")
            self.s.query("CREATE TABLE authors(authorId, familyName, givenNames, affiliatedToUnit, employedInUnit )")
            self.s.query("CREATE TABLE authors_publications(author_id, publication_id)")
            self.s.query("CREATE TABLE journals(issn, points, letter, journal)")
            self.s.query("CREATE VIEW v AS SELECT a.familyName, a.givenNames, a.authorId, p.pointsShare, p.publicationId, p.title, p.year, p.authors, j.letter, j.points, j.journal FROM journals j, authors a , publications p , authors_publications ap WHERE ap.author_id=a.authorId AND ap.publication_id=p.publicationId AND j.issn=p.parentId AND p.kind='Article';")
            self.s.query("CREATE TABLE authors_shares(author_id, publication_id, share)")
            #self.s.query("CREATE VIEW v AS SELECT a.familyName, a.givenNames, a.authorId, p.pointsShare, p.publicationId, p.title, p.kind, p.year, p.authors, j.letter, j.points, j.journal FROM journals j, authors a , publications p , authors_publications ap WHERE ap.author_id=a.authorId AND ap.publication_id=p.publicationId AND (j.issn=p.parentId OR p.kind='Chapter' OR p.kind='Book') ;")
        except: 
            pass
        self.s.query("DELETE FROM publications")
        self.s.query("DELETE FROM authors")
        self.s.query("DELETE FROM authors_publications")
        self.s.query("DELETE FROM journals")


# }}}
    def _build_journals_db(self):# {{{
        ''' Import punktacjaczasopism.pl database '''

        with open('journals.csv', 'r') as csvfile:
            reader = csv.reader(csvfile, delimiter=';')
            data=[]
            for i in reader:
                data.append([ i[0], int(i[1]), i[2], i[3] ])
        self.s.executemany('INSERT INTO journals VALUES (?,?,?,?)', data)
# }}}
    def _authors_as_string(self,a):# {{{
        ''' For authors_details, compact info: "author1,author2" '''

        if self.anonymize==1:
            return "autor1,autor2..." # anonymization, for now
        else:
            authors=[]
            for i in a['authors']:
                authors.append(i['familyName'])

            return ",".join(authors)

# }}}
    def _publication_record(self,a):# {{{
        ''' 
        Since PBN doesn't report the share of an author in the article, we take
        1/number_of_authors as a share
        '''

        if a['kind']=='Article':
            parentId=a['journal']['issn'].strip()
            parentTitle=a['journal']['title']['value'].strip()

        elif a['kind']=='Chapter':
            parentId=a['book']['isbn'].strip()
            parentTitle=a['book']['title'].strip()

        elif a['kind']=='Book':
            parentId=a['isbn'].strip()
            parentTitle=a['title'].strip()


        try:
            points=self.s.query("SELECT points FROM journals where issn=?", (parentId,))[0]['points']
        except:
            points=0

        z=[aa.strip() for aa in (a['firstSystemIdentifier'] , " ".join(a['title'].split()[0:5])+" ..." , a['kind'] , a['publicationDate'] , parentId, parentTitle, self._authors_as_string(a)) ]

        number_of_authors=self._calc_number_of_authors(a)
        z.append("{:.2f}".format(points * 1/number_of_authors))
        return z

# }}}
    def _calc_number_of_authors(self,json_record):# {{{
        ''' 
        We will share the points for the article amongst the authors.
        OtherContributors are from another institution and they don't count: if
        the article is worth 15 points, then each institutions shares their 15
        amongst their authors 
        '''

        if len(json_record['authors']) < 1:
            count_authors=1
        else:
            count_authors=len(json_record['authors'])

        return count_authors


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
            return [str(aa).strip() for aa in (a['pbnId'], a['familyName'], a['givenNames'], a['affiliatedToUnit'], a['employedInUnit'])]
        except:
            raise Exception("Problem with this author", a)
# }}}
    def _main(self):# {{{
        ''' 
        Need to be taken under account:
        PBN data contains leading/ending spaces: "issn": " 1234" 
        PBN data contains such authors: "familyName": "Kowalski", "givenNames": "Jan" and "familyName": "Jan", "givenNames": "Kowalski" 
        '''

        self._sqlite_init()
        self._build_journals_db()

        publications=[]
        authors=OrderedDict()
        authors_publications=[]

        for json_record in self.json.read(self.institution):
            p=self._publication_record(json_record)
            publications.append(p)
            for author in json_record['authors']:
                a=self._author_record(author)
                authors[a[0]]=tuple(a)
                if p[2]=='Article':
                    authors_publications.append((a[0],p[0]))
        self.s.executemany('INSERT INTO publications VALUES (?,?,?,?,?,?,?,?)', publications)
        self.s.executemany('INSERT INTO authors VALUES (?,?,?,?,?)', authors.values())
        self.s.executemany('INSERT INTO authors_publications VALUES (?,?)', set(authors_publications))

        if self.anonymize==1:
            self.s.query("UPDATE authors set familyName=authorId, givenNames='anonim'")
# }}}
    def _plot_data(self):# {{{
        plot_data=self.s.query("SELECT familyName, givenNames, authorId, round(sum(pointsShare),2) AS pointsShare FROM v GROUP BY familyName ORDER BY pointsShare DESC ")
        self.json.write(plot_data,"plot_data.json")
#}}}

# }}}

z=Swiatowid()
#z.s.all_v()
z.s.all_publicatons()
#z.s.all_journals()
#z.s.all_authors_publications()
#z.s.all_authors()
