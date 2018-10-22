from collections import OrderedDict
from subprocess import Popen,PIPE
import os
import csv
import argparse
import sys
import sqlite3
import inspect
import json
import time
from hashlib import sha1

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

# }}}
    def _argparse(self):# {{{
        parser = argparse.ArgumentParser(description='Opcje dla swiatowida. Zacznij od opcji -g, gdzie uzyskasz dalsze szczegóły.')

        parser.add_argument('-a' , help="Anonimizacja autorów"            , required=False , action='store_true')
        parser.add_argument('-g' , help="Pobierz publications.json z PBN" , required=False , action='store_true')
        parser.add_argument('-l' , help="Wyświetl bazę danych"            , required=False , action='store_true')
        parser.add_argument('-p' , help="Przetwarzaj publications.json"   , required=False , action='store_true')

        args = parser.parse_args()

        if args.a:
            self.anonymize=1
        if args.g:
            self._get_publications_json()
        if args.l:
            self.dump_sqlite=1
        if args.p:
            self._process_publications()
# }}}
    def _get_publications_json(self): # {{{
        try:
            PBN_KEY=os.environ['PBN_KEY']
            PBN_ID=os.environ['PBN_ID']
        except:
            print('''
Najtrudniejsze jest pierwsze uruchomienie ponieważ należy uzyskać
X-Auth-API-Key (PBN_KEY) z PBN. Taki klucz może uzyskać tylko osoba z funkcją
importer publikacji z danej instytucji. Klucz zamawia się przez
https://pbn-ms.opi.org.pl > Helpdesk.

PBN_ID to identyfikator instytucji w PBN. Pojawia się on w wielu miejscach, np.
w stopce strony https://pbn-ms.opi.org.pl (po zalogowaniu).

Po uzyskaniu klucza należy wyeksportować dwie zmienne shellowe 
(najwygodniej eksportować je przez ~/.bashrc):

export PBN_KEY="XXXXXXXXX-XXXXXXXXXXXX-XXXXXXXXX-XXXXXXXXX" 
export PBN_ID=125                                                                                         

Jeżeli zmienne istnieją, swiatowid przejdzie do dalszej procedury.
''')
            sys.exit()

# Wygeneruj klucz API
# Dodaj do niego godzinę i datę w formacie HHddMMYYYY (np dla godz. 8:35 w dniu 7 czerwca 2018 roku dodajemy „0807062018”)
# użyj funkcji hashujacej sha1 do zakodowania klucza z doklejoną godziną i datą


        key_date=PBN_KEY+time.strftime("%H%d%m%Y")
        encrypted=sha1(key_date.encode("utf-8")).hexdigest()

        print('\n\ncurl -X GET "https://pbn-ms.opi.org.pl/pbn-report-web/api/v2/search/institution/json/'+PBN_ID+'?page=0&pageSize=999999999&children=false" -H "X-Auth-API-Key: '+encrypted+'" > publications.json')
        print("\n\n")
        Popen('curl -X GET "https://pbn-ms.opi.org.pl/pbn-report-web/api/v2/search/institution/json/'+PBN_ID+'?page=0&pageSize=999999999&children=false" -H "X-Auth-API-Key: '+encrypted+'" > publications.json', shell=True)

# }}}
    def _sqlite_reset(self):# {{{

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
            try:
                a['journal']['issn']=a['journal']['eissn']
            except:
                pass

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
            print("\nBrak publications.json. Uruchom\npython3 swiatowid.py -g")
            sys.exit()
            
# }}}
    def _process_publications(self):# {{{
        ''' 
        Need to be taken under account:
        PBN data contains leading/ending spaces: "issn": " 1234" 
        PBN data contains such authors: "familyName": "Kowalski", "givenNames": "Jan" and "familyName": "Jan", "givenNames": "Kowalski" 
        '''

        self._sqlite_reset()
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
        plot_data=self.s.query("SELECT familyName, givenNames, authorId, round(sum(points),2) AS points FROM v GROUP BY authorId ORDER BY points DESC ")
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
<page> <h1>Światowid</h1> </page>
    <table id=plot_table> '''+"".join(h)+'''</table>
<author-details></author-details>
<script src="js/swiatowid.js"></script>
</html>''')
        print('''OK! Możesz umieścić ten folder w środowisku http + php-sqlite i oglądać plot.html.''')
#}}}
    def _dump_tables(self):# {{{
        try:
            if self.dump_sqlite==1:
                s=Sqlite("swiatowid.sqlite")
                s.select_publicatons()
                s.select_authors_publications()
                s.select_authors()
                s.select_v()
        except:
            pass
# }}}


z=Swiatowid()
