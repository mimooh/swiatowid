# add book to cuvier, then chapter
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
import re
from hashlib import sha1
from include import Psql

# psql cia -c "select * from  cuvier_artykul_autor";
# psql cia -c "select * from  cuvier_artykuly";
# psql cia -c "SELECT name,pbnId from pracownicy order by pbnId,name";
# sqlite3 swiatowid.sqlite "select * from authors_publications"
# sqlite3 swiatowid.sqlite "select * from publications where kind='książka'"
# sqlite3 swiatowid.sqlite "select * from publications where kind='artykuł'"
# sqlite3 swiatowid.sqlite "select * from publications order by kind"
# sqlite3 swiatowid.sqlite "select * from authors"
# sqlite3 swiatowid.sqlite "select * from authors_publications"
# sqlite3 swiatowid.sqlite "select distinct jezyk from publications "

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
        self.p=Psql()
        self.isbn=11111111
        self.anonymize=0
        self.json=Json()
        self._sgsp()
        exit()
        self._argparse()
        self._dump_tables()

# }}}

    def _sgsp_importer(self):# {{{
        ''' 
        Need to be taken under account:
        PBN data contains leading/ending spaces: "issn": " 1234" 
        '''

        publications=[]
        authors_publications=[]

        for json_record in self._publications: 
            r=self._publication_record(json_record)
            p=r[0]
            publications.append(p)
            if len(r[1]) > 0:
                publications.append(r[1])
            for kolejnosc,author in enumerate(json_record['authors']):
                a=self._author_record(author)
                authors_publications.append((a[0],p[-1],kolejnosc))
        self.s.executemany('INSERT INTO publications VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', publications)
        self.s.executemany('INSERT INTO authors_publications VALUES (?,?,?)', set(authors_publications))

# }}}
    def _sgsp_pbnids(self):#{{{
        ''' W bazie SGSP mam kolumne pbnid na identyfikatory autorow. '''
        for i in self.s.query("SELECT authorId,familyName,givenNames FROM authors"):
            print("UPDATE pracownicy SET pbnid={} WHERE nazwisko='{}' AND imie='{}';".format(i['authorId'], i['familyName'], i['givenNames']));
#}}}
    def _sgsp_pg_publications(self):#{{{
        print("\c cia")
        print("COPY cuvier_artykuly (tytul,jezyk,rok_publikacji,wersja,volume,issue,strona_od,strona_do,licencja,doi,kind,issn,isbn,parentId,konferencja,scopus,wos,othercontributors,parentTitle,abstrakt,publicationId) FROM stdin;")

        for i in self.s.query("select * from publications"):
            v=[]
            for vv in i.values():
                if type(vv)!=str:
                    vv='\\N'
                v.append(vv)
            print("\t".join(v))
        print("\.")
        print("")

#}}}
    def _sgsp_pg_authors(self):#{{{
        '''
        1954416|PBN-R:807049
        1770894|PBN-R:875262
        Ogrodnik Paweł|PBN-R:697638

        # sqlite3 swiatowid.sqlite "select * from authors"
        # sqlite3 swiatowid.sqlite "select * from authors_publications where publication_id='PBN-R:254749' order by kolejnosc"
        # sqlite3 swiatowid.sqlite "select distinct publicationId from publications"

        granule: PBN-R:254749 Karol Kreński ✓ , Adam Krasuski ✓ , Marcin Szczuka ✓ , Stanisław Łazowy ✓ 

        psql cia -c "select * from  cuvier_artykul_autor";
        psql cia -c "select * from  cuvier_artykuly";
        psql cia -c "select * from pracownicy";
        psql cia -c "alter table cuvier_artykul_autor add column pbnauthor ";
        psql cia -c "alter table pracownicy drop column pbnid";
        '''

        # print("\c cia")
        # print("COPY cuvier_artykuly (tytul,jezyk,rok_publikacji,wersja,volume,issue,strona_od,strona_do,licencja,doi,kind,issn,isbn,parentId,konferencja,scopus,wos,othercontributors,parentTitle,abstract,publicationId) FROM stdin;")
        # PBN-R:254749
        # psql cia -c "select id,tytul,publicationId  from  cuvier_artykuly where publicationid='PBN-R:254749'";

        #  id  |                 tytul                  | publicationid 
        # 1644 | Granular Knowledge Discovery Framework | PBN-R:254749
        # psql cia -c "select * from  pracownicy";

        #   id  | id_artykulu | id_autora | kolejnosc | obcy | afiliacja | modifier |  modified  | jest_redaktorem | pbnart | pbnauthor 

        #  1258 |        2033 |           |           |      |           |          | 2018-10-23 |                 |        |          
        # TODO: NON SGSP


        for i in self.s.query("select distinct publicationId as ii from publications"):
            for j in self.s.query("select * from authors_publications WHERE publication_id=? order by kolejnosc", (i['ii'],)):

                article_id=self.p.query("SELECT id from cuvier_artykuly where publicationId=%s", (j['publication_id'],))[0]['id']
                aa=self.p.query("SELECT id,name from pracownicy where pbnauthor=%s", (j['author_id'],))
                try:
                    aid=aa[0]['id']
                    is_obcy=0
                    afiliacja='SGSP'
                except:
                    aid='obcy'
                    is_obcy=1
                    afiliacja=''

                if aid=='obcy':
                    #psql cia -c "select * from  cuvier_obcy_autorzy";
                    #psql cia -c "select * from  cuvier_artykul_autor";
                    #psql cia -c "\d cuvier_artykul_autor";
                    z=self.p.query("select * from  cuvier_obcy_autorzy where name=%s", (j['author_id'],))
                    if len(z) == 0:
                        self.p.query("insert into cuvier_obcy_autorzy(name) values(%s)", (j['author_id'],))
                    z=self.p.query("select id from cuvier_obcy_autorzy where name=%s", (j['author_id'],))
                    aid=z[0]['id']
                # self.p.querydd('''
                # INSERT INTO cuvier_artykul_autor(
                # id_artykulu , id_autora , kolejnosc      , obcy    , afiliacja , jest_redaktorem , pbnart  , pbnauthor) values(
                # %s          , %s        , %s             , %s      , %s        , %s              , %s      , %s)'''             ,
                # (article_id , aid       , j['kolejnosc'] , is_obcy , afiliacja , 0               , i['ii'] , j['author_id'])
                # )

                self.p.query('''
                INSERT INTO cuvier_artykul_autor(
                id_artykulu , id_autora , kolejnosc      , obcy    , afiliacja , jest_redaktorem , pbnart  , pbnauthor) values(
                %s          , %s        , %s             , %s      , %s        , %s              , %s      , %s)'''             ,
                (article_id , aid       , j['kolejnosc'] , is_obcy , afiliacja , 0               , i['ii'] , j['author_id'])
                )


#}}}

    def _sgsp(self):#{{{
        self._sqlite_reset()
        self._read_json()
        #self._build_journals_db()
        self._process_authors()
        self._sgsp_importer()
        #self._sgsp_pg_publications()
        self._sgsp_pg_authors()
        #self._sgsp_pbnids()
#}}}
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
            self._sqlite_reset()
            self._read_json()
            self._build_journals_db()
            self._process_authors()
            self._plot_data()
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

        self.s.query("CREATE TABLE publications(tytul,jezyk,rok_publikacji,wersja,volume,issue,strona_od,strona_do,licencja,doi,kind,issn,isbn,parentId,konferencja,scopus,wos,othercontributors,parentTitle,abstrakt,publicationId)")
        self.s.query("CREATE TABLE authors(authorId, familyName, givenNames, affiliatedToUnit, employedInUnit )")
        self.s.query("CREATE TABLE authors_publications(author_id, publication_id, kolejnosc)")
        self.s.query('''
             CREATE VIEW v AS SELECT a.familyName, a.givenNames, a.authorId, p.parentTitle, p.publicationId, p.kind
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

    def _read_json(self):# {{{
        try:
            self._publications=self.json.read("publications.json")['works']
        except:
            print("\nBrak publications.json. Uruchom\npython3 swiatowid.py -g")
            sys.exit()
            
# }}}
    def _process_authors(self):# {{{
        ''' 
        Need to be taken under account:
        PBN data contains such authors: "familyName": "Kowalski", "givenNames": "Jan" and "familyName": "Jan", "givenNames": "Kowalski" 
        '''

        authors=OrderedDict()
        fixme=[]
        for json_record in self._publications: 
            for author in json_record['authors']:
                a=self._author_record(author)
                authors[a[0]]=tuple(a)
        self.s.executemany('INSERT INTO authors VALUES (?,?,?,?,?)', authors.values())

        if self.anonymize==1:
            self.s.query("UPDATE authors set familyName=authorId, givenNames='anonim'")


# }}}
    def _plot_data(self):# {{{
        return
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
<page> <h1>SGSP</h1> </page>
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

    def _article(self,a):#{{{
        # psql cia -c "\d cuvier_artykuly";

        try:
            a['pages'].strip()
            a['pages'].replace(" ", "")
            pages=a['pages'].split("-")
        except:
            pass
        try:
            i=pages[1]
        except:
            pages=[0, 0]

        try:
            a['journal']['issn']=a['journal']['eissn']
        except:
            pass

        record=[]
        record.append(a['sanitizedTitle'].strip())
        record.append(a['lang'].strip())
        record.append(a['publicationDate'].strip())
        record.append(None) # a['wersja'].strip())
        try:
            record.append(a['volume'].strip())
        except:
            record.append(0)
        try:
            record.append(a['issue'].strip())
        except:
            record.append(0)
        record.append(pages[0])
        record.append(pages[1])
        record.append(None) # a['licencja'].strip())
        record.append(a['doi'].strip())
        record.append('artykuł')
        record.append(a['journal']['issn'].strip())
        record.append(None)
        record.append(a['journal']['issn'].strip())
        try:
            record.append(a['conference']['name'].strip())
            record.append(a['conference']['scopusIndexed'])
            record.append(a['conference']['webOfScienceIndexed'])
        except:
            record.append(None) 
            record.append(None) 
            record.append(None) 

        try:
            record.append(a['otherContributors'])
        except:
            record.append(None)
        record.append(a['sanitizedTitle'].strip())
        try:
            record.append(a['abstracts'][0]['value'])
        except:
            record.append(None)
        record.append(a['firstSystemIdentifier'].strip())

        record=self._sanitize_record(record)
        return record
#}}}
    def _book(self,a):#{{{
        # psql cia -c "\d cuvier_artykuly";
        try:
            wydawca=a['publisherName']
        except:
            wydawca=None

        if not re.match("[\w]", a['firstSystemIdentifier']):
            a['firstSystemIdentifier']="ID-{}".format(self.isbn)
            self.isbn+=1

        if not re.match("\d", a['isbn']):
            a['isbn']="{}".format(self.isbn)
            self.isbn+=1
        a['isbn']=re.sub("[^\w]", "", a['isbn'])

        record=[]
        record.append(a['sanitizedTitle'].strip())
        try:
            record.append(a['lang'].strip())
        except:
            record.append(None)
        record.append(a['publicationDate'].strip())
        record.append(None) # a['wersja'].strip())
        record.append(None) # a['volume'].strip())
        record.append(None) # a['issue'].strip())
        record.append(None) # a['strona_od'].strip())
        record.append(None) # a['strona_do'].strip())
        record.append(None) # a['licencja'].strip())
        record.append(a['doi'].strip())
        record.append('książka')
        record.append(None) # a['issn'].strip())
        record.append(a['isbn'].strip())
        record.append(wydawca)
        try:
            record.append(a['conference']['name'].strip())
            record.append(a['conference']['scopusIndexed'])
            record.append(a['conference']['webOfScienceIndexed'])
        except:
            record.append(None) 
            record.append(None) 
            record.append(None) 

        try:
            record.append(a['otherContributors'])
        except:
            record.append(None)
        record.append(a['sanitizedTitle'].strip())
        try:
            record.append(a['abstracts'][0]['value'])
        except:
            record.append(None)
        record.append(a['firstSystemIdentifier'].strip())

        record=self._sanitize_record(record)
        return record
#}}}
    def _chapter(self,a):#{{{
        # psql cia -c "\d cuvier_artykuly";

        record=[]
        record.append(a['sanitizedTitle'].strip())
        record.append(a['lang'].strip())
        record.append(a['publicationDate'].strip())
        record.append(None) # a['wersja'].strip())
        record.append(None) # a['volume'].strip())
        record.append(None) # a['issue'].strip())
        record.append(None) # a['strona_od'].strip())
        record.append(None) # a['strona_do'].strip())
        record.append(None) # a['licencja'].strip())
        record.append(a['doi'].strip())
        record.append('rozdział')
        record.append(None) # a['issn'].strip())
        record.append(None) # a['isbn'].strip())
        record.append(a['book']['firstSystemIdentifier'].strip())
        try:
            record.append(a['conference']['name'].strip())
            record.append(a['conference']['scopusIndexed'])
            record.append(a['conference']['webOfScienceIndexed'])
        except:
            record.append(None) 
            record.append(None) 
            record.append(None) 

        try:
            record.append(a['otherContributors'])
        except:
            record.append(None)
        record.append(a['sanitizedTitle'].strip())
        try:
            record.append(a['abstracts'][0]['value'])
        except:
            record.append(None)
        record.append(a['firstSystemIdentifier'].strip())

        record=self._sanitize_record(record)
        return record
#}}}
    def _sanitize_record(self,record):#{{{
        z=[]
        for i in record:
            if type(i)==str:
                z.append(re.sub('[„"”]', '', i))
            else:
                z.append(i)
        return z
#}}}
    def _publication_record(self,a):# {{{
        # psql cia -c "\d cuvier_artykuly";
        # y jest tylko na okolicznosc chapter[book]
        y=[]

        if a['kind']=='Article':
            x=self._article(a)

        elif a['kind']=='Book':
            x=self._book(a)

        elif a['kind']=='Chapter':
            y=self._book(a['book'])
            x=self._chapter(a)

        #print(x[11], len(x))
        return (x,y)


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

        a['familyName']=a['familyName'].replace(" ", "").title()
        try:
            a['givenNames']=a['givenNames'].split(" ")[0].title();
        except:
            pass

        if not 'familyName' in a:
            a['familyName']='Brak';
        
        if not 'givenNames' in a:
            a['givenNames']='Brak';

        #print(a['familyName'])
        #print(a)

        if a['affiliatedToUnit']==True:
            try:
                # psql cia -c "SELECT name,pbnauthor from pracownicy where nazwisko ~ 'Gawro'"
                aa=self.p.query("SELECT pbnauthor from pracownicy where nazwisko=%s AND imie=%s", (a['familyName'], a['givenNames']))
                if len(aa) > 0:
                    a['pbnId']=aa[0]['pbnauthor']
            except:
                a['pbnId']=a['familyName']+" "+a['givenNames']
        else:
            a['pbnId']=a['familyName']+" "+a['givenNames']

        if not 'pbnId' in a: 
            a['pbnId']=a['familyName']+" "+a['givenNames']

        #print(a['pbnId'])
        
        return [str(aa).strip() for aa in (a['pbnId'], a['familyName'], a['givenNames'], a['affiliatedToUnit'], a['employedInUnit'])]
# }}}

z=Swiatowid()
