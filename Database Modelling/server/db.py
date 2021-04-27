import sqlite3
from flask.cli import with_appcontext
import logging
import time
from datetime import datetime



# helper function that converts query result to json, after cursor has executed query
def to_json(cursor):
    results = cursor.fetchall()
    headers = [d[0] for d in cursor.description]
    return [dict(zip(headers, row)) for row in results]


# Error class for when a key is not found
class KeyNotFound(Exception):
    def __init__(self, message=None):
        Exception.__init__(self)
        if message:
            self.message = message
        else:
            self.message = "Key/Id not found"

    def to_dict(self):
        rv = dict()
        rv['message'] = self.message
        return rv


# Error class for when request data is bad
class BadRequest(Exception):
    def __init__(self, message=None, error_code=400):
        Exception.__init__(self)
        if message:
            self.message = message
        else:
            self.message = "Bad Request"
        self.error_code = error_code

    def to_dict(self):
        rv = dict()
        rv['message'] = self.message
        return rv


"""
Wraps a single connection to the database with higher-level functionality.
Holds the DB connection
"""


class DB:
    def __init__(self, connection):
        self.conn = connection
        logging.basicConfig(level=logging.DEBUG)


    # Simple example of how to execute a query against the DB.
    # Again never do this, you should only execute parameterized query
    # See https://docs.python.org/3/library/sqlite3.html#sqlite3.Cursor.execute
    # This is the qmark style:
    # cur.execute("insert into people values (?, ?)", (who, age))
    # And this is the named style:
    # cur.execute("select * from people where name_last=:who and age=:age", {"who": who, "age": age})
    def run_query(self, query):
        c = self.conn.cursor()
        c.execute(query)
        res = to_json(c)
        self.conn.commit()
        return res

    # Run script that drops and creates all tables
    def create_db(self, create_file):
        print("Running SQL script file %s" % create_file, flush=True)
        logging.info('testing info log')
        with open(create_file, "r") as f:
            self.conn.executescript(f.read())
        return "{\"message\":\"created\"}"


    def add_artist(self, post_body):
        try:
            artist_id = post_body["artist_id"]
            artist_name = post_body["artist_name"]
            if "country" in post_body:
                country = post_body["country"]
            else:
                country = None
        except KeyError as e:
            raise BadRequest(message="Required attribute is missing")

        c = self.conn.cursor()

        artist_statement = "INSERT OR IGNORE INTO artist VALUES (?, ?, ?)"
        artist_values = [artist_id, artist_name, country]
        c.execute(artist_statement, artist_values)

        self.conn.commit()


        return "{\"message\":\"artist inserted\"}"


    def add_album(self, post_body):
        try:
            album_id = post_body["album_id"]
            album_name = post_body["album_name"]
            release_year = post_body["release_year"]
            artist_ids = post_body["artist_ids"]
            song_ids = post_body["song_ids"]
        except KeyError as e:
            raise BadRequest(message="Required attribute is missing")
        if isinstance(song_ids, list) is False or isinstance(artist_ids, list) is False:
            print("song_ids or artist_ids are not lists")
            raise BadRequest("song_ids or artist_ids are not lists")
        c = self.conn.cursor()



        album_statement = "INSERT OR IGNORE INTO album VALUES (?, ?, ?)"
        album_values = [album_id, album_name, release_year]
        c.execute(album_statement, album_values)


        for artist_id in artist_ids: 
            created_statement = "INSERT OR IGNORE INTO release VALUES (?, ?)"
            created_values = [album_id, artist_id]
            c.execute(created_statement, created_values)



        for song_id in song_ids: 
            ordering = int(time.mktime(datetime.now().timetuple()))
            created_statement = "INSERT OR IGNORE INTO tracklist VALUES (?, ?, ?)"
            created_values = [album_id, song_id, ordering]
            c.execute(created_statement, created_values)

        self.conn.commit()
        return "{\"message\":\"album inserted\"}"



    # Takes in json post_body and inserts a song, and potentially an artist and album
    # The loader returns 201 response signifying errorless insertion.
    # See the API documentation for more details on what is expected.
    def add_song_ms2(self, post_body):
        """
        Loads a new song (and possibly a new album/artist) into the database.
        """
        # =======MS1 data========
        try:
            song_id = post_body["song_id"]
            song_name = post_body["song_name"]
            length = post_body["length"]
            artist_ids = post_body["artist_ids"]
        except KeyError as e:
            raise BadRequest(message="Required attribute is missing")
        if isinstance(artist_ids, list) is False:
            raise BadRequest("artist_ids is not a list")

        c = self.conn.cursor()

        song_statement = "INSERT or IGNORE INTO song VALUES (?, ?, ?)"
        song_values = [song_id, song_name, length]
        c.execute(song_statement, song_values)


        for artist_id in artist_ids: 
            created_statement = "INSERT OR IGNORE INTO created VALUES (?, ?)"
            created_values = [artist_id, song_id]
            c.execute(created_statement, created_values)


        self.conn.commit()
        return "{\"message\":\"song inserted\"}"









    # Takes in json post_body and inserts a song, and potentially an artist and album
    # The loader returns 201 response signifying errorless insertion.
    # See the API documentation for more details on what is expected.
    def add_song(self, post_body):
        """
        Loads a new song (and possibly a new album/artist) into the database.
        """
        try:
            song_id = post_body["song_id"]
            song_name = post_body["song_name"]
            length = post_body["length"]
            artist_ids = post_body["artist_ids"]
        except KeyError as e:
            raise BadRequest(message="Required attribute is missing")
        if isinstance(artist_ids, list) is False:
            raise BadRequest("artist_ids is not a list")

        c = self.conn.cursor()

        song_statement = "INSERT or IGNORE INTO song VALUES (?, ?, ?)"
        song_values = [song_id, song_name, length]
        c.execute(song_statement, song_values)


        #add to created table

        for artist_id in artist_ids: 
            created_statement = "INSERT OR IGNORE INTO created VALUES (?, ?)"
            created_values = [artist_id, song_id]
            c.execute(created_statement, created_values)


        self.conn.commit()
        return "{\"message\":\"song inserted\"}"



    """
    Add a new playlist. 
    """
    def add_playlist(self, post_body):
        try:
            playlist_id = post_body["playlist_id"]
            playlist_name = post_body["playlist_name"]
            author_name = post_body["author_name"]
            song_ids = post_body["song_ids"]
        except KeyError as e:
            raise BadRequest(message="Required attribute is missing")
        if isinstance(song_ids, list) is False:
            print("song_ids is not a list")
            raise BadRequest("song_ids is not a lists")
        c = self.conn.cursor()


        playlist_statement = "INSERT OR IGNORE INTO playlist VALUES (?, ?, ?)"
        playlist_values = [playlist_id, playlist_name, author_name]
        c.execute(playlist_statement, playlist_values)


        for song_id in song_ids: 
            #ordering = int(time.mktime(datetime.now().timetuple()))
            created_statement = "INSERT OR IGNORE INTO playlist_songs VALUES (?, ?)"
            created_values = [playlist_id, song_id]
            c.execute(created_statement, created_values)

        self.conn.commit()


        return "{\"message\":\"playlist inserted\"}"


    """
    Add a new play event. 
    """
    def add_play(self, post_body):
        try:
            date = post_body["date"]
            song_id = post_body["song_id"]
            play_count = post_body["play_count"]
        except KeyError as e:
            raise BadRequest(message="Required attribute is missing")
        playlist_id = None
        album_id = None
        has_playlist = False
        has_album = False
        if "playlist_id" in post_body and "album_id" in post_body:
            raise BadRequest(message="Both playlist and album is specified")
        elif "playlist_id" in post_body:
            playlist_id = post_body["playlist_id"]
            has_playlist = True
        elif "album_id" in post_body:
            album_id = post_body["album_id"]
            has_album = True
        c = self.conn.cursor()

        play_values = [date, song_id, playlist_id, album_id]


        exists_statement = "SELECT play_count FROM play WHERE (date = ?) and (song_id = ?) and (playlist_id is ?) and (album_id is ?)"
        c.execute(exists_statement, play_values)
        res = to_json(c)

        print("TESTING")

        print(len(res))

        if len(res) != 0:
            print("Adding DUPLICATES new count is " + str(play_count))

            play_statement = "UPDATE play set play_count=play_count + ? where (date = ?) and (song_id = ?) and (playlist_id is ?) and (album_id is ?)"
            play_values = [play_count, date, song_id, playlist_id, album_id]
        else:

            play_statement = "INSERT INTO play VALUES (?, ?, ?, ?, ?)"

            play_values = [date, play_count, song_id, playlist_id, album_id]


        c.execute(play_statement, play_values)

        self.conn.commit()

        return "{\"message\":\"play inserted\"}"


    """
    Returns a song's info
    raise KeyNotFound() if song_id is not found
    """
    def find_song(self, song_id):
        c = self.conn.cursor()
        # Your query should fetch (song_id, name, length, artist_name, album_name) based on song_id

        to_find = song_id
        find_statement = "SELECT distinct s.song_id, s.song_name, s.length FROM song s where (s.song_id = ?)"
        c.execute(find_statement, (to_find,))
        res = to_json(c)



        if len(res) == 0:
            raise KeyNotFound("Song Id not found")
            return

        find_statement2 = "SELECT artist_id from created c where (c.song_id = :id)"
        c.execute(find_statement2, {"id": res[0]["song_id"]})
        res[0]["artist_ids"] = [x[0] for x in c.fetchall()]
        res[0]["artist_ids"].sort()


        find_statement3 = "SELECT album_id from tracklist t where (t.song_id = :id)"
        c.execute(find_statement3, {"id": to_find})
        res[0]["album_ids"] = [x[0] for x in c.fetchall()] 
        res[0]["album_ids"].sort()



        return res




    """
    Returns all an album's songs
    raise KeyNotFound() if album_id not found
#     """
    def find_songs_by_album(self, album_id):
        c = self.conn.cursor()
        # Your query should fetch (song_id, name, length, artist name, album name) based on album_id

        #some tests do not include length, some do. included her.

        ret = []

        to_find = album_id


        verify = "SELECT album_id from album a where (a.album_id = :id)"
        c.execute(verify, {"id": to_find})
        res = to_json(c)
        if len(res) == 0:
            raise KeyNotFound("Album Id not found")
            return


        find_statement = "SELECT distinct a.album_name FROM album a where (a.album_id = ?)"

        c.execute(find_statement, (to_find,))

        res = to_json(c)

        album_name = res[0]["album_name"]


        find_statement2 = "SELECT song_id from tracklist t where (t.album_id = :id) order by t.ordering"
        c.execute(find_statement2, {"id": to_find})
        song_id_list = [x[0] for x in c.fetchall()]

        for song_id in song_id_list:
            to_find = song_id
            find_statement = "SELECT distinct s.song_id, s.song_name, s.length FROM song s where (s.song_id = ?)"
            c.execute(find_statement, (to_find,))
            res = to_json(c)
            res[0]["album_name"] = album_name

            find_statement2 = "SELECT artist_id from created c where (c.song_id = :id)"
            c.execute(find_statement2, {"id": song_id})
            res[0]["artist_ids"] = [x[0] for x in c.fetchall()]
            res[0]["artist_ids"].sort()

            ret.append(res[0])



        self.conn.commit()
        return ret


    """
    Returns all an artists' songs
    raise KeyNotFound() if artist_id is not found
    """
    def find_songs_by_artist(self, artist_id):
        c = self.conn.cursor()


        ret = []

        to_find = artist_id

        verify = "SELECT artist_id from artist a where (a.artist_id = :id)"
        c.execute(verify, {"id": to_find})
        res = to_json(c)
        if len(res) == 0:
            raise KeyNotFound("Artist Id not found")
            return


        find_statement2 = "SELECT song_id from created c where (c.artist_id = :id)"
        c.execute(find_statement2, {"id": to_find})
        song_id_list = [x[0] for x in c.fetchall()]
        song_id_list.sort() #song_ids sorted ascending


        for song_id in song_id_list:
            to_find = song_id
            find_statement = "SELECT distinct s.song_id, s.song_name, s.length FROM song s where (s.song_id = ?)"
            c.execute(find_statement, (to_find,))
            res = to_json(c)

            find_statement2 = "SELECT artist_id from created c where (c.song_id = :id)"
            c.execute(find_statement2, {"id": song_id})
            res[0]["artist_ids"] = [x[0] for x in c.fetchall()]
            res[0]["artist_ids"].sort()

            ret.append(res[0])


 

        self.conn.commit()
        return ret

    """
    Returns a album's info
    raise KeyNotFound() if album_id is not found
    """
    def find_album(self, album_id):
        c = self.conn.cursor()

        to_find = album_id

        find_statement = "SELECT distinct al.album_id, al.album_name, al.release_year FROM album al where (al.album_id = ?) order by al.album_id asc "

        c.execute(find_statement, (to_find,))

        res = to_json(c)


        if len(res) == 0:
            raise KeyNotFound("Album Id not found")
            return


        find_statement2 = "SELECT artist_id from release r where (r.album_id = :id)"
        c.execute(find_statement2, {"id": to_find})
        res[0]["artist_ids"] = [x[0] for x in c.fetchall()]
        res[0]["artist_ids"].sort()


        find_statement3 = "SELECT song_id from tracklist t where (t.album_id = :id) order by t.ordering"
        c.execute(find_statement3, {"id": to_find})
        res[0]["song_ids"] = [x[0] for x in c.fetchall()] 

        self.conn.commit()
        return res

    """
    Returns a album's info
    raise KeyNotFound() if artist_id is not found 
    if artist exist, but there are no albums then return an empty result (from to_json)
    """
    def find_album_by_artist(self, artist_id):
        c = self.conn.cursor()

        ret = []

        to_find = artist_id


        verify = "SELECT artist_id from artist a where (a.artist_id = :id)"
        c.execute(verify, {"id": to_find})
        res = to_json(c)
        if len(res) == 0:
            raise KeyNotFound("Artist Id not found")
            return


        find_statement2 = "SELECT album_id from release r where (r.artist_id = :id)"
        c.execute(find_statement2, {"id": to_find})
        album_id_list = [x[0] for x in c.fetchall()]
        album_id_list.sort() #song_ids sorted ascending

        print("album id list is ", flush=True)
        print(album_id_list)


        for album_id in album_id_list:
            to_find = album_id
            find_statement = "SELECT distinct a.album_id, a.album_name, a.release_year FROM album a where (a.album_id = ?)"
            c.execute(find_statement, (to_find,))
            res = to_json(c)

            find_statement2 = "SELECT artist_id from release r where (r.album_id = :id)"
            c.execute(find_statement2, {"id": to_find})
            res[0]["artist_ids"] = [x[0] for x in c.fetchall()]
            res[0]["artist_ids"].sort()

            ret.append(res[0])

 

        return ret
   

    """
    Returns a artist's info
    raise KeyNotFound() if artist_id is not found 
    """
    def find_artist(self, artist_id):
        c = self.conn.cursor()
        to_find = artist_id

        find_statement = "SELECT distinct a.artist_id, a.artist_name, a.artist_country as country FROM artist a where (a.artist_id = ?) order by a.artist_id asc "

        c.execute(find_statement, (to_find,))

        res = to_json(c)

        if len(res) == 0:
            raise KeyNotFound("artist Id not found")
            return
 
        else:
            self.conn.commit()
            return res


    """
    Returns the average length of an artist's songs (artist_id, avg_length)
    raise KeyNotFound() if artist_id is not found 
    """
    def avg_song_length(self, artist_id):
        c = self.conn.cursor()

        to_find = artist_id

        find_statement = "SELECT a.artist_id, AVG(s.length) as avg_length FROM song s join created c on (s.song_id = c.song_id) join artist a on (a.artist_id = c.artist_id) where (a.artist_id = ?) "

        c.execute(find_statement, (to_find,))


        res = to_json(c)

        if len(res) == 0:
            raise KeyNotFound("artist Id not found")
            return
 
        else:  
            self.conn.commit()
            return res

    """
    Returns the number of singles an artist has (artist_id, cnt_single)
    raise KeyNotFound() if artist_id is not found 
    """
    def cnt_singles(self, artist_id):
        c = self.conn.cursor()

        to_find = artist_id

        find_statement = "SELECT distinct b.artist_id, COUNT(*) as cnt_single FROM (song NATURAl JOIN created) AS b LEFT JOIN (artist NATURAL JOIN tracklist) AS a ON (b.song_id  = a.song_id) WHERE (album_id IS NULL AND b.artist_id = ?)"
        c.execute(find_statement, (to_find,))

        res = to_json(c)

        if len(res) == 0:
            raise KeyNotFound("artist Id not found")
            return
 
        else:
            self.conn.commit()
            return res

    """
    Returns top (n=num_artists) artists based on total length of songs
    """
    def top_length(self, num_artists):
        c = self.conn.cursor()

        limit = num_artists

        find_statement = "SELECT distinct a.artist_id, sum(s.length) as total_length FROM artist a join created c on (a.artist_id = c.artist_id) join song s on (s.song_id = c.song_id) group by a.artist_id order by total_length desc limit ?  "

        c.execute(find_statement, (limit,))

        res = to_json(c)

        if len(res) == 0:
            raise KeyNotFound("no artists found")
            return
 
        else:
            self.conn.commit()
            return res


    """
    Returns an array/list of album_ids where the album is by one artist
    and all songs are by the same single artist.
    """
    def solo_albums(self):
        c = self.conn.cursor()

        find_statement = "SELECT r.album_id from release r where r.album_id not in (SELECT t.album_id as non_solo from tracklist as t inner join created as c on (t.song_id = c.song_id) inner join release as r on (r.album_id = t.album_id) group by r.album_id, c.song_id having (count(*)!=1 or (r.artist_id != c.artist_id)))"

        c.execute(find_statement, ())

        res = [x[0] for x in c.fetchall()]

        res.sort()

        if len(res) == 0:
            raise KeyNotFound("no albums found")
            return


        self.conn.commit()
        return res



    """
    Returns the top song for a given date. 
    Expects (play_count, song_id) as the return object
    The test data does not account for ties/ have ties. if you want to break them use song_id ascending.
    """
    def top_song(self, check_date):
        c = self.conn.cursor()

        find_statement = "SELECT p.song_id as song_id, SUM(p.play_count) as play_count from play p where (p.date = ?) group by p.song_id order by play_count desc limit 1"
        c.execute(find_statement, (check_date,))
        res = to_json(c)
        
        if len(res) == 0:
            raise KeyNotFound("Invalid date or date format")
            return
 
        else:
            self.conn.commit()
            return res



        

    """
    For a given song and date return the source (playlist_id, album_id or neither) 
    that contributed to the most plays for a song. Remember multiple play events can come for a 
    song, source, and date. See the MS instructions for a hint on removing elements from 
    the to_json result (will make your life easier to issue a single query).
    Expects one of the following return types:
     (play_count, playlist_id)
     (play_count, album_id)
     (play_count)   
    """
    def top_source(self, song_id, check_date):
        c = self.conn.cursor()

        find_statement = "SELECT play_count, album_id, playlist_id FROM play p where (p.date = ?) and (p.song_id = ?)"
        c.execute(find_statement, (check_date,song_id,))
        res = to_json(c)

        find_statement = "SELECT (MAX(play_count)) as play_count FROM play p where playlist_id is not null and (p.date = ?) and (p.song_id = ?)"
        c.execute(find_statement, (check_date,song_id,))
        res = to_json(c)
        playlist = res[0]["play_count"] 

        find_statement = "SELECT (MAX(play_count)) as play_count FROM play p where album_id is not null and (p.date = ?) and (p.song_id = ?)"
        c.execute(find_statement, (check_date,song_id,))
        res = to_json(c)
        album = res[0]["play_count"] 

        find_statement = "SELECT (MAX(play_count)) as play_count FROM play p where album_id is null and playlist_id is null and (p.date = ?) and (p.song_id = ?)"
        c.execute(find_statement, (check_date,song_id,))
        res = to_json(c)
        none = res[0]["play_count"] 

        options = [playlist, album, none]

        (max_val,index) = max((v,i) for i,v in enumerate(options) if v is not None)

        find_statement = "SELECT playlist_id, album_id, play_count FROM play p where play_count = ? and (p.date = ?) and (p.song_id = ?)"
        c.execute(find_statement, (max_val,check_date,song_id,))
        res = to_json(c)
        if res[0]['playlist_id'] is None:
            del(res[0]['playlist_id'])
        if res[0]['album_id'] is None:
            del (res[0]['album_id'])

        if len(res) == 0:
            raise KeyNotFound("song Id not found")
            return
 
        else:
            self.conn.commit()
            return res





    """
    For a given date find the country with the most plays. Here will
    count a play for a country, by only considering the artists associated with the songs 
    (and not the album it was played on).  
    Expects (country, play_count)
    """
    def top_country(self, check_date):
        c = self.conn.cursor()

        #one song can have multiple artists from the same country--as per Ed, we only count them once

        create_statement = "CREATE TEMPORARY TABLE duplicates AS SELECT c.song_id, COUNT(c.artist_id) as artist_count from created c natural join artist a group by c.song_id, a.artist_country having COUNT(c.artist_id) > 1"

        c.execute(create_statement, ())

        find_statement = "SELECT a.artist_country as country, SUM(p.play_count/IFNULL(d.artist_count, 1)) as play_count from (play p natural join created c natural join artist a left join duplicates d on d.song_id = p.song_id) where (p.date = ?) group by a.artist_country order by play_count desc limit 1"


        c.execute(find_statement, (check_date,))
        res = to_json(c)

        if len(res) == 0:
            raise KeyNotFound("Invalid date or date format")
            return
 
        else:
            self.conn.commit()
            return res




