use constant MP2 => 1;

################################################################################

sub get_request {

	our $r        = $_[0];
	our $apr      = Apache::Request -> new ($r);
	our %_COOKIES = Apache::Cookie  -> fetch;
	our %_REQUEST = %{$apr -> parms};

	delete $_REQUEST{__suicide} if (defined($_REQUEST{__suicide}));

}

################################################################################

sub send_http_header {}

################################################################################

sub set_cookie {

	my $cookie = Apache::Cookie -> new ($r, @_);
		
	$r -> err_headers_out -> add ('Set-Cookie' => $cookie -> as_string);

}

################################################################################

sub upload_file_dimensions {

	my ($upload) = @_;
	
	($upload -> fh, $upload -> filename, $upload -> size, $upload -> type);

}

################################################################################

sub _ok {0};

################################################################################

BEGIN {

	require Apache::RequestRec;
	require Apache::RequestUtil;
	require Apache::RequestIO;
	require Apache::Const;
	require Apache::Upload;
	require Apache::Cookie;

	Apache -> push_handlers (PerlChildInitHandler => \&sql_reconnect );
	Apache -> push_handlers (PerlChildExitHandler => \&sql_disconnect);

	$ENV {PERL_JSON_BACKEND} = 'JSON::PP';		

	loading_log "Apache::RequestRec $Apache::RequestRec::VERSION, ok.\n";

}

1;