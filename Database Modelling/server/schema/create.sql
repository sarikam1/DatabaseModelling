drop table if exists duplicates;

drop table if exists playlist_songs;

drop table if exists release;

drop table if exists tracklist;

drop table if exists created;

drop table if exists play;

drop table if exists playlist;

drop table if exists song;

drop table if exists album;
drop table if exists artist;

create table artist(artist_id INT primary key, artist_name varchar(15) not null, artist_country varchar(15));
create table album(album_id int primary key, album_name varchar(15) not null, release_year int not null);

create table song(song_id int primary key, song_name varchar(15) not null, length int not null);

create table playlist(playlist_id int primary key, playlist_name varchar(15) not null, author_name varchar(15));

create table play(date varchar(15) not null, play_count int not null, song_id int not null, playlist_id int, album_id int, foreign key(song_id) references song(song_id), foreign key(playlist_id) references playlist(playlist_id), foreign key(album_id) references album(album_id));

create table created(artist_id int references artist(artist_id), song_id int references song(song_id));

create table tracklist(album_id int references album(album_id), song_id int references song(song_id), ordering int not null);

create table release(album_id int references album(album_id), artist_id int references artist(artist_id));

create table playlist_songs(playlist_id int references playlist(playlist_id), song_id int references song(song_id));








