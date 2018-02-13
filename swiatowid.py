from collections import OrderedDict
from subprocess import Popen,PIPE
import os
import csv
import argparse
import sys
import sqlite3
import inspect
import json
import xmltodict

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

    def select_publicatons(self):
        print("\n======= SELECT * FROM publications ========")
        dd(self.query("SELECT * FROM publications"))

    def select_authors(self):
        print("\n======= SELECT * FROM authors ========")
        dd(self.query("SELECT authorId,familyName,givenNames FROM authors"))

    def select_authors_publications(self):
        print("\n======= SELECT * FROM authors_publications ========")
        dd(self.query("SELECT * FROM authors_publications"))

    def select_v(self):
        print("\n======= SELECT * FROM v ========")
        dd(self.query("SELECT * FROM v"))

# }}}

class Swiatowid():
    def __init__(self):# {{{
        self.anonymize=0
        self.json=Json()
        self._argparse()
        self._dump_tables()
        self._import_from_blue("blue.xml")

# }}}
    def _argparse(self):# {{{
        parser = argparse.ArgumentParser(description='Options for swiatowid')

        parser.add_argument('-m' , help="Process publications.json"                                                                , required=False   , action='store_true')
        parser.add_argument('-a' , help="Anonymize authors"                                                                        , required=False   , action='store_true')
        parser.add_argument('-g' , help="Get publications.json from PBN"                                                           , required=False   , action='store_true')
        parser.add_argument('-d' , help="See the sqlite database"                                                                  , required=False   , action='store_true')
        parser.add_argument('-x' , help="Import <export.xml> from https://pbn.nauka.gov.pl/sedno-webapp/institutions/exportSearch" , required=False )

        args = parser.parse_args()

        if args.a:
            self.anonymize=1
        if args.d:
            self.dump_sqlite=1
        if args.g:
            self._get_publications_json()
        if args.m:
            self._main()
        if args.x:
            self._import_from_blue(args.x)
# }}}
    def _get_publications_json(self): # {{{
        try:
            PBN_KEY=os.environ['PBN_KEY']
            PBN_ID=os.environ['PBN_ID']
        except:
            print('''
First run is the hardest since you need to obtain the X-Auth-API-Key (PBN_KEY)
from PBN. You need to be the publication importer for your institution and ask
for the key via https://pbn-ms.opi.org.pl > Helpdesk

PBN_ID is your institution ID in PBN. It can be found in many places, e.g. in
the footer of https://pbn-ms.opi.org.pl after you have logged in.

After you obtained the key, export the two PBN variables in your shell
environment (best via ~/.bashrc). 

export PBN_KEY="XXXXXXXXX-XXXXXXXXXXXX-XXXXXXXXX-XXXXXXXXX" 
export PBN_ID=125                                                                                         
''')
            sys.exit()

        Popen('curl -X GET "https://pbn-ms.opi.org.pl/pbn-report-web/api/v2/search/institution/json/$PBN_ID?page=0&pageSize=999999999&children=false" -H "X-Auth-API-Key: $PBN_KEY" > publications.json', shell=True)

# }}}
    def _sqlite_init(self):# {{{

        try:
            os.remove("swiatowid.sqlite")
        except:
            pass

        self.s=Sqlite("swiatowid.sqlite")

        self.s.query("CREATE TABLE journals(issn, points, letter, journal)")
        self.s.query("CREATE TABLE publications(publicationId, title, kind, year, parentId, parentTitle, authors, points, letter)")
        self.s.query("CREATE TABLE authors(authorId, familyName, givenNames, affiliatedToUnit, employedInUnit )")
        self.s.query("CREATE TABLE authors_publications(author_id, publication_id)")
        self.s.query('''
             CREATE VIEW v AS SELECT a.familyName, a.givenNames, a.authorId, p.points, p.letter, p.parentTitle, p.publicationId, p.title, p.kind, p.year, p.authors 
             FROM authors a , publications p , authors_publications ap
             WHERE 
             ap.author_id=a.authorId 
             AND ap.publication_id=p.publicationId; ''')

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

            return " ".join(authors)

# }}}
    def _shorten_title(self, title):# {{{
        ''' Title will have max 5 words. Also comas bother me for a reason now. '''
        title=title.replace(",","")
        words=title.split()
        short=" ".join(words[0:5])
        if len(words) > 5:
            short+=" ..."
        return short
# }}}
    def _publication_record(self,a):# {{{

        if a['kind']=='Article':
            parentId=a['journal']['issn'].strip()
            parentTitle=a['journal']['title']['value'].strip()

        elif a['kind']=='Chapter':
            parentId=a['book']['isbn'].strip()
            parentTitle=a['book']['title'].strip()

        elif a['kind']=='Book':
            parentId=a['isbn'].strip()
            parentTitle=a['title'].strip()

        articleTitle=self._shorten_title(a['title'])
        parentTitle=self._shorten_title(parentTitle)

        try:
            z=self.s.query("SELECT points,letter FROM journals where issn=?", (parentId,))[0]
            points=str(z['points'])
            letter=str(z['letter'])
        except:
            points=str(0)
            letter='-'

        z=[aa.strip() for aa in (a['firstSystemIdentifier'] , articleTitle , a['kind'] , a['publicationDate'] , parentId, parentTitle, self._authors_as_string(a), points, letter) ]
        return z

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
            #print("Problem with this author", a)
            return ['?', '?', '?', '?', '?']
# }}}
    def _publications_data(self):# {{{
        try:
            return self.json.read("publications.json")['works']
        except:
            print("\nMissing publications.json. Run\npython3 swiatowid.py -g")
            sys.exit()
            
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

        for json_record in self._publications_data(): 
            p=self._publication_record(json_record)
            publications.append(p)
            for author in json_record['authors']:
                a=self._author_record(author)
                authors[a[0]]=tuple(a)
                authors_publications.append((a[0],p[0]))

        self.s.executemany('INSERT INTO publications VALUES (?,?,?,?,?,?,?,?,?)', publications)
        self.s.executemany('INSERT INTO authors VALUES (?,?,?,?,?)', authors.values())
        self.s.executemany('INSERT INTO authors_publications VALUES (?,?)', set(authors_publications))

        if self.anonymize==1:
            self.s.query("UPDATE authors set familyName=authorId, givenNames='anonim'")

        self._plot_data()
# }}}
    def _plot_data(self):# {{{
        plot_data=self.s.query("SELECT familyName, givenNames, authorId, round(sum(points),2) AS points FROM v GROUP BY familyName ORDER BY points DESC ")
        h=[]
        for i,j in enumerate(plot_data):
            record=[]
            record.append('\n\t\t<tr>')
            record.append('<td>{}'.format(i+1))
            record.append('<td><author id={}>{} {}</author>'.format(j['authorId'], j['familyName'], j['givenNames']))
            record.append('<td>{}'.format(j['points']))
            record.append('<td><svg width="1000" height="20" id=svg{}>'.format(i+1))
            record.append('<rect y="0" x="0" height="20" width="{}" style="color:#000000; opacity:0.8; fill:#004488; stroke:#0088ff; stroke-width:1" />'.format(j['points']))
            record.append('</svg>')
            h.append(''.join(record))
            

        with open("plot.html", "w") as f: 
            f.write('''<HTML><HEAD>
<META http-equiv=Content-Type content='text/html; charset=utf-8' />
<link href="https://fonts.googleapis.com/css?family=Roboto" rel="stylesheet">
<LINK rel='stylesheet' type='text/css' href='css.css'>
<script src="js/jquery.js"></script>
</HEAD>
<BODY>
<page> <h1>SGSP okresie 2011-2017</h1> </page>
    <table id=plot_table> '''+"".join(h)+'''</table>
<author-details></author-details>
<script src="js/swiatowid.js"></script>
</html>''')
        print('''Done! Place this directory in a http + php environment and see plot.html.''')
#}}}
    def _dump_tables(self):# {{{
        try:
            if self.dump_sqlite==1:
                self.s.select_publicatons()
                self.s.select_authors_publications()
                self.s.select_authors()
                self.s.select_v()
        except:
            pass
# }}}
    def _fix_authors(self):# {{{
        ''' Poor PBN design that authors are not always a list. We get:
        * [ author1, author2 ]
        * [ author1, author2, author3 ]
        * author1                -- not a list by a dict
        * NULL                   -- author even missing from xml!

        Also, we are missing given-names at least. Poor, poor xml design.
        '''

        fixed=OrderedDict()
        failed_authors=OrderedDict()
        failed_authors['affiliated']=[]
        failed_authors['not-affiliated']=[]
        for work,records in self.works.items():
            fixed[work]=list()
            for i in records:
                try:
                    if isinstance(i['author'],dict):
                        z=list()
                        z.append(i['author'])
                        i['author']=z
                except:
                    i['author']=[OrderedDict()]

                
                fine_authors=[]
                for a in i['author']: 
                    try:
                        author=OrderedDict([('given-names', a['given-names']), ('family-name', a['family-name']), ('system-identifier', a['system-identifier']['#text']), ('affiliated-to-unit', a['affiliated-to-unit'])])
                    except:
                        try:
                            if a['affiliated-to-unit']=='true':
                                failed_authors['affiliated'].append((a, i['publication-date'],i['title']))
                            else:
                                failed_authors['not-affiliated'].append((a, i['publication-date'],i['title']))
                        except:
                            failed_authors['not-affiliated'].append((a, i['publication-date'],i['title']))
                        
                    fine_authors.append(a)
                i['author']=fine_authors
                fixed[work].append(i)
                
        print("== affiliated ==")
        dd(failed_authors['affiliated'])
        print("== not-affiliated ==")
        dd(failed_authors['not-affiliated'])

        return fixed
# }}}
    def _import_from_blue(self,xml_file): # {{{
        with open(xml_file) as f:
            doc = xmltodict.parse(f.read())
        del doc['works']['@xmlns']
        del doc['works']['@pbn-unit-id']
        self.works=doc['works']
        self._fix_authors()
        # for i in self.works['chapter']:
        #     for j in i['author']:
        #         print(j)
        #         #print(j['family-name'])

# }}}

z=Swiatowid()
