#!/usr/bin/perl

use strict;
use XML::Parser;
use Data::Dumper;
use DBI;
use Date::Parse qw(str2time);

my $dbname="osm.sqlite";

unlink $dbname;

my $dbh = DBI->connect("dbi:SQLite:dbname=$dbname"
	,""
	,""
	,{ PrintError => 1, AutoCommit => 0 });

# We need just very few of these tokens actually as global values.
my($nd_ref, $nd_way_id, $nd_ordering, $nd_last_way_id);
my($way_id, $way_changeset, $way_visible, $way_userstring_id, $way_timestamp);
my($node_id, $node_lat, $node_lon, $node_changeset, $node_visible, $node_userstring_id, $node_version, $node_uid, $node_timestamp);
my($relation_id, $relation_changeset, $relation_visible, $relation_userstring_id, $relation_timestamp);
my($member_type_id, $member_ref, $member_role, $member_relation_id, $member_ordering, $member_last_relation_id);
my($tag_k, $tag_v, $tag_parenttype_id, $tag_parent_id, $tag_ordering);
my($changeset_id, $changeset_closed_at, $changeset_max_lat, $changeset_uid, $changeset_max_lon, $changeset_open, $changeset_created_at, $changeset_min_lat, $changeset_min_lon, $changeset_userstring_id);

&init_db();

my %count;
my @tagpath;
my $callcounter;

my $p = new XML::Parser(
	Handlers=>
	{
		Start=> \&start_handler, 
		End=> \&end_handler, 
		Default=>\&default_handler, 
		Char => \&char_handler
	}, 
	ErrorContext => 2);
$p->parsefile($ARGV[0]);
#$p->parse(*stdin);
#$p->parsefile("border-germany.osm");
$dbh->commit();

print Dumper(\%count);

sub default_handler()
{
	my ($p, $data) =@_;
	my($line);
	
	$line=$p->current_line;
	#print "DEFAULT_HANDLER: ".$data."\n";
}

sub start_handler()
{
	my ($p, $data, %attr_vals) =@_;
	my($line, $key, $sql);
		my($tag_last_parenttype_id, $tag_last_parent_id);
	$callcounter++;
	
	$line=$p->current_line;
	#print "START: ".$data." "."\nattr-vals: ".join(" ",keys %attr_vals)."\n";
	#print Dumper(\%attr_vals)."\n";
	foreach $key (keys %attr_vals)
	{
		$count{'attr'}{$data}{$key}++;
	}
	# The XML tag path (without counters) as an array is updated
	push @tagpath, $data;
	if ($data eq "node")
	{
		# Preprocessing XML data
		$node_id=$attr_vals{'id'};
		$node_lat=$attr_vals{'lat'};
		$node_lon=$attr_vals{'lon'};
		$node_changeset="\'".&sql_escape($attr_vals{'changeset'})."\'";
		if($attr_vals{'visible'} eq "true")
		{
			$node_visible=1;
		}
		elsif($attr_vals{'visible'} eq "false")
		{
			$node_visible=0;
		}
		else
		{
##			print("ERROR of OSM compliance: node visibility is not defined!\n");
			$node_visible=2;
		}
		$node_userstring_id=&autouserid($attr_vals{'user'});
		$node_timestamp=str2time($attr_vals{'timestamp'});
		# These attributes I found in planet.osm in 2010-02.
		$node_uid=$attr_vals{'uid'};
		if($node_uid eq "")
		{
			$node_uid=0;
		}
		$node_version=$attr_vals{'version'};

		$sql=("INSERT INTO node (id, lat, lon, changeset, visible, userstring_id, timestamp, uid, version) VALUES ("
			.$node_id.", "
			.$node_lat.", "
			.$node_lon.", "
			.$node_changeset.", "
			.$node_visible.", "
			.$node_userstring_id.", "
			.$node_timestamp.", "
			.$node_uid.", "
			.$node_version
			.");");
		if(! $dbh->do($sql))
			{
				print $dbh->err;
				print $sql."\n";
			};
 		$tag_parent_id=$node_id;
	}
	elsif ($data eq "way")
	{
		$way_id=$attr_vals{'id'};
		$way_changeset="\'".&sql_escape($attr_vals{'changeset'})."\'";
		if($attr_vals{'visible'} eq "true")
		{
			$way_visible=1;
		}
		elsif($attr_vals{'visible'} eq "false")
		{
			$way_visible=0;
		}
		else
		{
			$way_visible=2;
		}
		$way_userstring_id=&autouserid($attr_vals{'user'});
		$way_timestamp=str2time($attr_vals{'timestamp'});

		$sql=
		("INSERT INTO way (id, changeset, visible, userstring_id, timestamp) VALUES ("
			.$way_id.", "
			.$way_changeset.", "
			.$way_visible.", "
			.$way_userstring_id.", "
			.$way_timestamp
			.");");
 		if(! $dbh->do($sql))
		{
			print $sql."\n";
			print $dbh->err."\n";
		}
 		$tag_parent_id=$way_id;
	}
	elsif($data eq "nd")
	{
		# Check: is this <nd> a child of a way?
		if ($tagpath[$#tagpath-1] eq "way")
		{
			# Ok, let's go ahead reading in way node references.
			$nd_ref=$attr_vals{'ref'};
			$nd_way_id=$way_id;
			if($nd_last_way_id == $way_id)
			{
				$nd_ordering++;
			}
			else
			{
				$nd_ordering=1;
			}
			$nd_last_way_id=$way_id;
			$dbh->do("INSERT INTO nd (ref, way_id, ordering) VALUES ("
				.$nd_ref.", "
				.$nd_way_id.", "
				.$nd_ordering
				.");");
		}
		else
		{
			print("ERROR: <nd> should be XML child of a way.\n");
		}
	}
	elsif($data eq "relation")
	{
		$relation_id=$attr_vals{'id'};
		if(defined($attr_vals{'changeset'}))
		{
			$relation_changeset=$attr_vals{'changeset'};
		}
		else
		{
			$relation_changeset=0;
		}
		if($attr_vals{'visible'} eq "true")
		{
			$relation_visible=1;
		}
		elsif($attr_vals{'visible'} eq "false")
		{
			$relation_visible=0;
		}
		else
		{
			$relation_visible=2;
		}
		$relation_userstring_id=&autouserid($attr_vals{'user'});
		$relation_timestamp=str2time($attr_vals{'timestamp'});

		$sql=("INSERT INTO relation (id, changeset, visible, userstring_id, timestamp) VALUES ("
			.$relation_id.", "
			.$relation_changeset.", "
			.$relation_visible.", "
			.$relation_userstring_id.", "
			.$relation_timestamp
			.");");
		if( ! $dbh->do($sql) )
		{
		 	print $sql."\n";
			print $dbh->err;
		}
 		$tag_parent_id=$relation_id;
	}
	elsif($data eq "member")
	{
		# Check: is this <member> a child of a relation?
		if ($tagpath[$#tagpath-1] eq "relation")
		{
			# Creating the SQL sub-select string
			$member_type_id=" ( SELECT id FROM membertype WHERE name='".$attr_vals{'type'}."') ";
			$member_ref=$attr_vals{'ref'};
			$member_role=$attr_vals{'role'};
			$member_relation_id=$relation_id;
			if($member_last_relation_id == $relation_id)
			{
				$member_ordering++;
			}
			else
			{
				$member_ordering=1;
			}
			$member_last_relation_id=$relation_id;
	$sql=("INSERT INTO member (membertype_id, ref, role, relation_id, ordering) VALUES ("
				.$member_type_id.", "
				.$member_ref.", "
				."\'".$member_role."\', "
				.$member_relation_id.", "
				.$member_ordering
				.");");
#	 	print $sql."\n";
	 		if(!$dbh->do($sql))
			{
				print $dbh->err."\n";
				print $sql."\n";
			}
		}
		else
		{
			print("ERROR: <member> should be XML child of a relation.\n");
		}
	}
	elsif($data eq "tag")
	{
		my ($parent);
		$parent=$tagpath[$#tagpath-1];
		$tag_k="\'".&sql_escape($attr_vals{'k'})."\'";
		$tag_v="\'".&sql_escape($attr_vals{'v'})."\'";
		$tag_parenttype_id=" ( SELECT id FROM tagparenttype WHERE name='".$parent."' ) ";
		#$tag_parent_id; This is the respective ID set in preferences changeset node way relation
		if ($parent eq "preferences") # I could do this with eval(), but this should be faster
		{
#			$tag_parent_id=$preferences_id;
		}
		elsif($parent eq "changeset")
		{
			$tag_parent_id=$changeset_id;
		}
		elsif($parent eq "node")
		{
			$tag_parent_id=$node_id;
		}
		elsif($parent eq "way")
		{
			$tag_parent_id=$way_id;
		}
		elsif($parent eq "relation")
		{
			$tag_parent_id=$relation_id;
		}
		if($tag_last_parent_id == $tag_parent_id and $tag_last_parenttype_id=$tag_parenttype_id)
		{
			$tag_ordering++;
		}
		else
		{
			$tag_ordering=1;
		}
		$tag_last_parent_id=$tag_parent_id;
		$tag_last_parenttype_id=$tag_parenttype_id;

		$sql="INSERT INTO tag (k, v, tagparenttype_id, tagparent_id, ordering) VALUES ("
			.$tag_k.", "
			.$tag_v.", "
			.$tag_parenttype_id.", "
			.$tag_parent_id.", "
			.$tag_ordering
		.");";
		if(!$dbh->do($sql))
		{
			print $sql."\n";
			print $dbh->err;
		}
	}
	elsif($data eq "changeset")
	{
		$changeset_id=$attr_vals{'id'};
		$changeset_closed_at=str2time($attr_vals{'closed_at'});
		$changeset_max_lat=$attr_vals{'max_lat'};
		$changeset_uid=$attr_vals{'uid'};
		$changeset_max_lon=$attr_vals{'max_lon'};
		if($attr_vals{'open'} eq "true")
		{
			$changeset_open=1;
		}
		else
		{
			$changeset_open=0;
		}
		$changeset_created_at=str2time($attr_vals{'created_at'});
		$changeset_min_lat=$attr_vals{'min_lat'};
		$changeset_min_lon=$attr_vals{'min_lon'};
		$changeset_userstring_id=&autouserid($attr_vals{'user'});
		$tag_parent_id=$changeset_id;

		$dbh->do("INSERT INTO changeset (id, closed_at, max_lat, uid, max_lon, open, created_at, min_lat, min_lon, userstring_id) VALUES ("
			.$changeset_id.", "
			.$changeset_closed_at.", "
			.$changeset_max_lat.", "
			.$changeset_uid.", "
			.$changeset_max_lon.", "
			.$changeset_open.", "
			.$changeset_created_at.", "
			.$changeset_min_lat.", "
			.$changeset_min_lon.", "
			.$changeset_userstring_id
			.");")
	}
	if($#tagpath >=1)
	{
		$count{'isparent'}{$tagpath[$#tagpath-1]}{$tagpath[$#tagpath-0]}++
	}
	if($callcounter%100000 == 0)
	{
		open(RESULT, ">", "osm-tmp-result.txt") || die "$!";
		print RESULT (Dumper(\%count));
		close(RESULT);
		$dbh->commit();
	}
}

sub end_handler()
{
	my ($p, $data) =@_;
	my($line);
	
	$line=$p->current_line;
	#print "END: ".$data."\n";
	pop @tagpath;
}

sub char_handler()
{
	my ($p, $data) =@_;
	my($line);

	#print "CHAR: ".$data."\n";
}

sub init_db()
{
	# Schema for OSM API v0.6
	my ($sth, $query, @updates);
	@updates=
		("CREATE TABLE IF NOT EXISTS node
		(
			id INTEGER PRIMARY KEY,
			lat REAL NOT NULL,
			lon REAL NOT NULL,
			changeset TEXT,
			visible BOOLEAN NOT NULL,
			userstring_id INTEGER,
			timestamp DATETIME,
			uid INTEGER,
			version INTEGER
		);",
		"CREATE TABLE IF NOT EXISTS way
		(
			id INTEGER PRIMARY KEY,
			changeset TEXT,
			visible BOOLEAN,
			userstring_id INTEGER,
			timestamp DATETIME
		);",
		"CREATE TABLE IF NOT EXISTS relation
		(
			id INTEGER PRIMARY KEY,
			changeset TEXT,
			visible BOOLEAN,
			userstring_id INTEGER,
			timestamp DATETIME
		);",
		"CREATE TABLE IF NOT EXISTS nd
		(
			ref INTEGER NOT NULL,
			way_id INTEGER NOT NULL,
			ordering INTEGER NOT NULL,
			PRIMARY KEY(ref,way_id,ordering)
		);",
		"CREATE TABLE IF NOT EXISTS member
		(
			-- type is normalized to membertype as foreign key
			membertype_id INTEGER NOT NULL,
			ref INTEGER NOT NULL,
			role TEXT,
			relation_id INTEGER NOT NULL,
			ordering INTEGER NOT NULL,
			PRIMARY KEY(relation_id, membertype_id, ref, role, ordering)
		);", 
		"CREATE TABLE IF NOT EXISTS tag
		(
			k TEXT NOT NULL,
			v TEXT NOT NULL,
			tagparenttype_id INTEGER NOT NULL,
			tagparent_id INTEGER NOT NULL,
			ordering INTEGER NOT NULL,
			PRIMARY KEY(k, v, tagparenttype_id, tagparent_id, ordering)
		);", 
		"CREATE TABLE IF NOT EXISTS membertype
		(
			id INTEGER PRIMARY KEY,
			name TEXT
		);", 
		"INSERT INTO membertype (id, name) VALUES (1, 'way');",
		"INSERT INTO membertype (id, name) VALUES (2, 'node');",
		"INSERT INTO membertype (id, name) VALUES (3, 'relation');",
		"CREATE TABLE IF NOT EXISTS tagparenttype
		(
			id INTEGER PRIMARY KEY,
			name TEXT
		);",
		"INSERT INTO tagparenttype (id, name) VALUES (1, 'preferences');",
		"INSERT INTO tagparenttype (id, name) VALUES (2, 'changeset');",
		"INSERT INTO tagparenttype (id, name) VALUES (3, 'node');",
		"INSERT INTO tagparenttype (id, name) VALUES (4, 'way');",
		"INSERT INTO tagparenttype (id, name) VALUES (5, 'relation');",
		"CREATE TABLE IF NOT EXISTS changeset
		(
			id INTEGER PRIMARY KEY,
			closed_at DATETIME,
			max_lat REAL,
			uid INTEGER,
			max_lon REAL,
			open BOOLEAN,
			created_at DATETIME,
			min_lat REAL,
			min_lon REAL,
			userstring_id INTEGER
		);",
		"CREATE TABLE IF NOT EXISTS userstring
		(
			id INTEGER PRIMARY KEY,
			name TEXT
		);"
		)
		;
		foreach $query (@updates)
		{
			$sth=$dbh->do($query);
		}
		$dbh->commit();
}

sub sql_escape()
{
	my $string=shift;
	$string=~ s/'/''/gis;

	return $string;
}

sub autouserid() # Creates a new user ID if necessary; returns in any case the user ID
{
	my $userstring=shift;
	my ($query, $sth, $hashref, $uid);
	$query="SELECT count(id) as c, id FROM userstring WHERE name =\'".&sql_escape($userstring)."\';";
	$sth=$dbh->prepare($query);
	$sth->execute;
	$hashref=$sth->fetchrow_hashref;
	if($$hashref{'c'} == 0)
	{
		$dbh->do("INSERT INTO userstring (name) VALUES (\'".&sql_escape($userstring)."\');");
		$dbh->commit;
		$query="SELECT count(id) as c, id FROM userstring WHERE name =\'".&sql_escape($userstring)."\';";
		$sth=$dbh->prepare($query);
		$sth->execute;
		$hashref=$sth->fetchrow_hashref;
	}
	return $$hashref{'id'};
}
