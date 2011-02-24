package Mixi::HS::Base;
use strict;
use warnings;

use base qw/Class::Accessor::Fast Class::Data::Inheritable/;

use Data::Dumper;

use DBI;
use List::Util qw/first shuffle/;
use List::MoreUtils qw/part all/;
use Net::HandlerSocket;
use SQL::Abstract;
use Scope::Session;

use Mixi::HS::Exception;

__PACKAGE__->mk_classdata('table_name');
__PACKAGE__->mk_classdata('columns');
__PACKAGE__->mk_classdata('indexes');
__PACKAGE__->mk_classdata('_hs');
__PACKAGE__->mk_classdata('_hs_w');
__PACKAGE__->mk_classdata('DEBUG');
__PACKAGE__->mk_classdata('auto_increment');

__PACKAGE__->mk_classdata('dbs');
__PACKAGE__->mk_classdata('_shard_num');
__PACKAGE__->mk_classdata('_pnotes_enable');
__PACKAGE__->mk_classdata('_cache');

use constant MASTER => 'master';
use constant SLAVE  => 'slave';

use constant DEFAULT_PORT      => 3306;
use constant DEFAULT_USER      => "root";
use constant DEFAULT_PASSWORD  => "";
use constant DEFAULT_HS_PORT   => 9998;
use constant DEFAULT_HS_PORT_W => 9999;

__PACKAGE__->_pnotes_enable(0);
__PACKAGE__->_cache( {} );

#FOR APACHE2
if ( exists $ENV{MOD_PERL_API_VERSION} && $ENV{MOD_PERL_API_VERSION} == 2 ) {
    eval { require Apache2::ServerUtil };
    if ( !$@ ) {
        __PACKAGE__->_pnotes_enable(1)
          if Apache2::ServerUtil::restart_count() > 1;
    }
}

#======================== Class Method ==========================
sub init {
    my $class = shift;
    my ( $dbs, $is_debug ) = @_;

    $class->dbs($dbs);
    $class->switch_shard(0);

    $class->_hs(   [] );
    $class->_hs_w( [] );

    $class->DEBUG($is_debug);
    $class->_load_schema_info;
    $class->_create_accessors;
    $class->_init_read_handlersocket;

    return $class;
}

sub switch_shard {
    my ( $class, $num ) = @_;
    if (   not defined $num
        || $num !~ /^\d+$/
        || $num >= scalar @{ $class->dbs } )
    {
        die "Invalid shard num: $num";
    }
    $class->_shard_num($num);
}

sub shard_count {
    my ($class) = @_;
    return scalar @{$class->dbs};
}

sub _get_master_db {
    my $class = shift;
    my $shard = $class->dbs->[ $class->_shard_num ];
    my ( $host, $port ) = split /:/, $shard->master, 2;
    return {
        host      => $host,
        port      => $port || DEFAULT_PORT,
        database  => $shard->database,
        user      => $shard->user || DEFAULT_USER,
        password  => $shard->password || DEFAULT_PASSWORD,
        hs_port   => $shard->hs_port || DEFAULT_HS_PORT,
        hs_port_w => $shard->hs_port_w || DEFAULT_HS_PORT_W,
    };
}

sub _get_slave_db {
    my $class  = shift;
    my $shard  = $class->dbs->[ $class->_shard_num ];
    my $slaves = $shard->slave;

    my ( $host, $port );
    if ( ref $slaves eq 'ARRAY' ) {
        my @candidate = shuffle @$slaves;
        ( $host, $port ) = split /:/, $candidate[0], 2;
    }
    else {
        ( $host, $port ) = split /:/, $slaves, 2;
    }

    return {
        host      => $host,
        port      => $port || DEFAULT_PORT,
        database  => $shard->database,
        user      => $shard->user || DEFAULT_USER,
        password  => $shard->password || DEFAULT_PASSWORD,
        hs_port   => $shard->hs_port || DEFAULT_HS_PORT,
        hs_port_w => $shard->hs_port_w || DEFAULT_HS_PORT_W,
    };
}

sub _get_hs {
    my $class = shift;
    my $hs    = $class->_hs->[ $class->_shard_num ];
    return $hs if defined $hs;
    $class->_init_read_handlersocket;
    $class->_hs->[ $class->_shard_num ];
}

sub _set_hs {
    my ( $class, $hs ) = @_;
    $class->_hs->[ $class->_shard_num ] = $hs;
}

sub _get_hs_w {
    my $class = shift;
    my $hs_w  = $class->_hs_w->[ $class->_shard_num ];
    return $hs_w if defined $hs_w;
    $class->_init_write_handlersocket;
    $class->_hs_w->[ $class->_shard_num ];
}

sub _set_hs_w {
    my ( $class, $hs_w ) = @_;
    $class->_hs_w->[ $class->_shard_num ] = $hs_w;
}

sub _create_accessors {
    my $class = shift;

    my @parts = part { $class->_is_pk($_) } @{ $class->columns };
    $class->mk_accessors( @{ $parts[0] }, '__from_db' );
    $class->mk_ro_accessors( @{ $parts[1] } );
}

sub _load_schema_info {
    my $class = shift;
    $class->_load_columns unless defined $class->columns;
    $class->_load_indexes unless defined $class->indexes;
}

sub _init_write_handlersocket {
    my $class = shift;
    my $db    = $class->_get_master_db;
    my $hs    = Net::HandlerSocket->new(
        {
            host => $db->{host},
            port => $db->{hs_port_w},
        }
    );
    $class->_set_hs_w($hs);
    my $columns = $class->columns;
    my $err =
      $hs->open_index( 0, $db->{database}, $class->table_name, 'PRIMARY',
        join( ",", @$columns ) );
    Mixi::HS::Exception->new( { error => $hs->get_error() } )->throw
      if $err != 0;
}

sub _init_read_handlersocket {
    my $class = shift;
    my $db    = $class->_get_slave_db;
    my $hs    = Net::HandlerSocket->new(
        {
            host => $db->{host},
            port => $db->{hs_port}
        }
    );
    $class->_set_hs($hs);
    my $indexes = $class->indexes;
    my $columns = $class->columns;
    my $i       = 0;
    foreach my $key ( keys %$indexes ) {
        my $err =
          $hs->open_index( ++$i, $db->{database}, $class->table_name, $key,
            join( ",", @$columns ) );
        Mixi::HS::Exception->new( { error => $hs->get_error() } )->throw
          if $err != 0;
        my $func_name = "find_by_"
          . ( $key eq 'PRIMARY' ? "pk" : join "_", @{ $indexes->{$key} } );
        print STDERR "func: $func_name\n" if $class->DEBUG;
        $class->_create_find_method( $func_name, $i ) unless $class->can($func_name);
    }
}

sub _create_find_method {
    my ( $class, $func_name, $index_num ) = @_;

    {
        no strict 'refs';
        my $method = "$class" . "::" . "$func_name";

        print STDERR "generate method: $method\n" if $class->DEBUG;

        *{$method} = sub {
            my ( $class, $params, $count, $offset ) = @_;
            print STDERR "$method invoked\n" if $class->DEBUG;
            $count  ||= 1;
            $offset ||= 0;
            my $res = $class->_execute_single_wrapper(
                'r',
                sub {
                    shift->execute_single( $index_num, '=', $params, $count,
                        $offset );
                }
            );
            shift @$res;
            my $columns = $class->columns;
            my $rows    = scalar(@$res) / scalar(@$columns);
            my @result;

            for ( my $i = 0 ; $i < $rows ; ++$i ) {
                my %obj = ( __from_db => 1 );
                for ( my $j = 0 ; $j < scalar @$columns ; ++$j ) {
                    $obj{ $columns->[$j] } =
                      $res->[ $i * scalar(@$columns) + $j ];
                }
                push @result, $class->new( \%obj );
            }
            return scalar(@result) <= 1 ? $result[0] : \@result;
        };
    }
}

sub _execute_single_wrapper {
    my ( $class, $r_or_w, $sub ) = @_;

    my $res   = [];
    my $times = 0;
    do {
        my $hs = $r_or_w eq 'r' ? $class->_get_hs : $class->_get_hs_w;
        Mixi::HS::Exception->new( { error => $hs->get_error() } )->throw
          if $times++ == 2;
        $res = $sub->($hs);

        if ( $res->[0] < 0 ) {
            $r_or_w eq 'r'
              ? $class->_init_read_handlersocket
              : $class->_init_write_handlersocket;
        }
        elsif ( $res->[0] > 0 ) {
            Mixi::HS::Exception->new( { error => $hs->get_error() } )->throw;
        }
    } while ( $res->[0] != 0 );

    return $res;
}

sub _load_columns {
    my ($class) = @_;

    print STDERR 'load_columns for ' . $class->table_name . "\n"
      if $class->DEBUG;

    my $rows = $class->select_all( SLAVE, "DESC " . $class->table_name );
    $class->columns(
        [
            map {
                $class->auto_increment( $_->{Field} )
                  if $_->{Extra} =~ /auto_increment/i;
                $_->{Field};
              } @{$rows}
        ]
    );
}

sub _load_indexes {
    my ($class) = @_;

    print STDERR 'load_indexes for ' . $class->table_name . "\n"
      if $class->DEBUG;

    my $rows =
      $class->select_all( SLAVE, 'SHOW INDEX FROM ' . $class->table_name );

    my %index;
    foreach my $row ( @{$rows} ) {
        my $index_name   = $row->{Key_name};
        my $seq_in_index = $row->{Seq_in_index} - 1;
        my $column_name  = $row->{Column_name};
        $index{$index_name} = [] if !exists $index{$index_name};
        $index{$index_name}[$seq_in_index] = $column_name;
    }
    $class->indexes( \%index );
}

sub _is_pk {
    my ( $class, $key ) = @_;
    my $pks = $class->indexes->{'PRIMARY'};
    return 1 if first { $key eq $_ } @$pks;
    return 0;
}

#=========================== raw ==============================
sub _get_dbh_key {
    my ( $class, $db ) = @_;
    join ":", map { $db->{$_} } qw/host port database/;
}

sub _get_dbh {
    my ( $class, $m_or_s ) = @_;
    my $db = $m_or_s eq SLAVE ? $class->_get_slave_db : $class->_get_master_db;

    my $key = $class->_get_dbh_key($db);
    my $dbh = $class->_notes($key);

    if ( !$dbh || !( $dbh->FETCH('Active') && $dbh->ping ) ) {
        print STDERR "new connect\n" if $class->DEBUG;
        eval {
            $dbh = DBI->connect(
                "DBI:mysql:database="
                  . $db->{database}
                  . ";host="
                  . $db->{host}
                  . ";port="
                  . ( $db->{port} || 3306 ),
                $db->{user},
                $db->{password} || "",
                { RaiseError => 1, }
            );
        };
        $class->_notes( $key => $dbh );
    }
    else {
        print STDERR "get cached dbh\n" if $class->DEBUG;
    }

    return $dbh;
}

sub _notes {
    my $class = shift;
    if ( $class->_pnotes_enable ) {
        print STDERR "Apache2 pnotes\n" if $class->DEBUG;
        return Apache2::RequestUtil->request->pnotes(@_);
    }
    elsif ( Scope::Session->is_started ) {
        print STDERR "Scope::Session pnotes\n" if $class->DEBUG;
        if (@_) {
            return Scope::Session->notes(@_);
        }
        else {
            return $Scope::Session::Notes::DATA_STORE;
        }
    }
    else {
        if ( scalar @_ == 1 ) {
            return $class->_cache->{ $_[0] };
        }
        elsif ( scalar @_ == 2 ) {
            $class->_cache->{ $_[0] } = $_[1];
        }
    }
}

sub execute {
    my ( $class, $m_or_s, $stmt, $bind ) = @_;
    if (   $m_or_s eq SLAVE
        && $stmt =~ /^ *(INSERT|UPDATE|DELETE|REPLACE|ALTER|TRUNCATE)/i )
    {
        die "Cannot execute $stmt on SLAVE!";
    }
    my $dbh = $class->_get_dbh($m_or_s);
    my $sth = $dbh->prepare($stmt);
    $sth->execute(@$bind);
    $sth;
}

sub select_all {
    my ( $class, $m_or_s, $stmt, $bind, $filter ) = @_;
    my $sth = $class->execute( $m_or_s, $stmt, $bind );
    my $result = $sth->fetchall_arrayref( {} );
    if ( $filter && ref $filter eq 'CODE' ) {
        $_ = $filter->($_) foreach @$result;
    }
    return $result;
}

sub select_row {
    my ( $class, $m_or_s, $stmt, $bind, $filter ) = @_;
    my $sth = $class->execute( $m_or_s, $stmt, $bind );
    my $result = $sth->fetchrow_hashref;
    if ( $filter && ref $filter eq 'CODE' && $result ) {
        $result = $filter->($result);
    }
    return $result;
}

sub select_one {
    my ( $class, $m_or_s, $stmt, $bind ) = @_;
    my $sth = $class->execute( $m_or_s, $stmt, $bind );
    my @row = $sth->fetchrow_array;
    return $row[0];
}

sub _get_last_insert_id {
    my ($class) = @_;
    return $class->select_one( MASTER, "SELECT LAST_INSERT_ID()" );
}

#==============================================================

sub search {
    my ( $class, $where, $order, $count, $offset ) = @_;
    my $sql = SQL::Abstract->new;
    my ( $stmt, @bind ) =
      $sql->select( $class->table_name, "*", $where, $order );
    $stmt .= " LIMIT $count"   if $count;
    $stmt .= " OFFSET $offset" if $offset;

    my $result = $class->select_all(
        SLAVE, $stmt,
        \@bind,
        sub {
            my $row = shift;
            $row->{__from_db} = 1;
            $class->new($row);
        }
    );
    return $result;
}

#======================== Instance Method ==========================
sub is_from_db {
    my $self = shift;
    return $self->__from_db;
}

sub update {
    my ($self) = @_;
    die "Cannot use this method" if !$self->is_from_db;
    my $res = $self->_execute_single_wrapper(
        'w',
        sub {
            shift->execute_single( 0, '=',
                [ map { $self->$_ } @{ $self->indexes->{PRIMARY} } ],
                1, 0, 'U', [ map { $self->$_ } @{ $self->columns } ] );
        }
    );
    return $res->[1];
}

sub create {
    my ($self) = @_;
    my $res = $self->_execute_single_wrapper(
        'w',
        sub {
            shift->execute_single( 0, '+',
                [ map { $self->$_ } @{ $self->columns } ],
                1, 0 );
        }
    );
}

sub delete {
    my ($self) = @_;
    my $res = $self->_execute_single_wrapper(
        'w',
        sub {
            shift->execute_single( 0, '=',
                [ map { $self->$_ } @{ $self->indexes->{PRIMARY} } ],
                1, 0, 'D' );
        }
    );
    return $res->[1];
}

sub create_auto_inc {
    my ($self) = @_;
    die "no auto_increment" unless $self->auto_increment;
    my $sql       = SQL::Abstract->new;
    my $fieldvals = $self->to_hash;
    my ( $stmt, @bind ) = $sql->insert( $self->table_name, $fieldvals );
    $self->execute( MASTER, $stmt, \@bind );
    return $self->_get_last_insert_id;
}

sub to_hash {
    my ($self) = @_;
    my %fieldvals;
    map { $fieldvals{$_} = $self->$_ } @{ $self->columns };
    return \%fieldvals;
}

sub inspect {
    my ($self) = @_;
    "["
      . join( ", ",
        map { $_ . " => '" . ( $self->$_ || "" ) . "'" } @{ $self->columns } )
      . "]";
}

1;
