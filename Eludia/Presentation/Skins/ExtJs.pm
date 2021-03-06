package Eludia::Presentation::Skins::ExtJs;

no warnings;

BEGIN {

	our $replacement = {};

}

################################################################################

sub options {

	return {
	
		core_unblock_navigation => $preconf -> {core_unblock_navigation},
		
		no_trunc_string => 1,
		
		no_server_html  => 1,
		
	};
	
}

################################################################################

sub __submit_href { "javaScript:submitFormNamed('$_[1]')" }

################################################################################

sub __adjust_button_href {

	my ($_SKIN, $options) = @_;
		
	my $js_action;
		
	if ($options -> {href} =~ /^javaScript\:/i) {
		
		$js_action = $'
		
	}
	else {

		$options -> {target} ||= 'center';
			
		$js_action = "nope('$options->{href}','$options->{target}')";

	}

	if ($options -> {confirm}) {	
		
		my $condition = 'confirm(' . $_JSON -> encode ($options -> {confirm}) . ')';
		
		if ($options -> {preconfirm}) {

			$condition = "!$options->{preconfirm}||($options->{preconfirm}&&$condition)";

		}

		$js_action = "if($condition){$js_action}";
		
	}

	$options -> {href} = "javascript:$js_action";

	if ((my $h = $options -> {hotkey}) && !$options -> {off}) {

		$h -> {type} = 'href';

		$h -> {js_code} = $js_action;

		hotkey ($h);

	}

}

################################################################################

sub js_set_select_option {

	my ($_SKIN, $name, $item, $fallback_href) = @_;
	
	'';

}

################################################################################

sub draw_dump_button { () }

################################################################################

sub register_hotkey {

	my ($_SKIN, $hashref) = @_;

	$hashref -> {label} =~ s{\&(.)}{<u>$1</u>} or return undef;
	
	return undef if $_REQUEST {__edit};

	my $c = $1;
		
	if ($c eq '<') {
		return 37;
	}
	elsif ($c eq '>') {
		return 39;
	}
	elsif (lc $c eq '�') {
		return 186;
	}
	elsif (lc $c eq '�') {
		return 222;
	}
	else {
		$c =~ y{����������������������������������������������������������������}{qwertyuiop[]asdfghjkl;'zxcvbnm,.qwertyuiop[]asdfghjkl;'zxcvbnm,.};
		return (ord ($c) - 32);
	}

}

################################################################################

sub static_path {

	my ($package, $file) = @_;
	my $path = __FILE__;

	$path    =~ s{\.pm}{/$file};

	return $path;

};

################################################################################

sub draw_auth_toolbar {

	my ($_SKIN, $options) = @_;

	return 'auth_toolbar000';

}

################################################################################

sub draw_page {

	$_REQUEST_VERBATIM {type} or return &{$_PACKAGE . 'draw_logon'} ();

	my ($_SKIN, $page) = @_;
	
	if ($page -> {body} =~ /\<html\>\s*$/ism) {
	
		$_REQUEST {__content_type} = 'text/html; charset=' . $i18n -> {_charset};

		return $page -> {body};
	
	}

	my $user_subset_menu = Data::Dumper::Dumper (
		
		&{$_PACKAGE . 'get_user_subset_menu'} ()
			
	);

	my $md5 = Digest::MD5::md5_hex ($user_subset_menu);

	$_REQUEST {__content_type} = 'text/javascript; charset=' . $i18n -> {_charset};

	my %hotkeys = ();

	foreach my $i (@{$page -> {scan2names}}) {

		my $key = (join '', map {my $x = $i -> {$_}; (ref $x ? $$x : $x) ? 1 : 0} qw (ctrl alt shift)) . $i -> {code};
		
		if ($i -> {data} -> {href} =~ /^javaScript\:/i) {
		
			$i -> {js_code} = $';
		
		}
		elsif ($i -> {data} -> {href}) {
		
			$i -> {js_code} = "nope($i->{data}->{href})";
			
		}

		$hotkeys {$key} = $i -> {js_code};

	}

	return "$_REQUEST{__script};ui.hotkeys=" . $_JSON -> encode (\%hotkeys) . ";ui.checkMenu('$md5');$page->{body};ui.target.doLayout();";

}

################################################################################

sub draw_vert_menu {

	my ($_SKIN, $name, $types, $level, $is_main) = @_;
	
	return 'draw_vert_menu';
	
}

################################################################################

sub draw_menu {

	my ($_SKIN, $_options) = @_;

	return 'draw_menu';

}

################################################################################

sub draw_toolbar_button {

	my ($_SKIN, $_options) = @_;

	return 'draw_toolbar_button';

}

################################################################################

sub draw_hr {};

################################################################################

sub draw_window_title {

	my ($_SKIN, $options) = @_;

	return $options -> {label};

}

################################################################################

sub draw_table {

	my ($_SKIN, $tr_callback, $list, $options) = @_;
	
	!exists $_REQUEST {__only_table} or $_REQUEST {__only_table} eq $options -> {name} or return '';

	$options -> {id}     ||= 0 + $options;
		
	my @rows = map {$_ -> {__field_values}} @$list;

	my $n = @rows ? $rows [0] -> {cnt} : 0;

	my $content = {
		
		success          => \1,
		
		cell_hrefs       => \%cell_hrefs,
		
		cell_href_prefix => $_REQUEST {__uri_root_common},
	
		root             => \@rows,
		
	};
	
	my @toolbar = ();
	
	foreach my $button (@{$options -> {top_toolbar} -> {buttons}}) {
	
		if ($button -> {off}) {
		
			next;		
		
		}
		elsif ($button -> {type} eq 'pager') {
		
			$content -> {$_} = $button -> {$_} foreach qw (total cnt);
		
		}
		else {
		
			push @toolbar, $button;

		}

	}
	
	my $toolbar = $_JSON -> encode (\@toolbar);

	my $data = $_JSON -> encode ($content);
	
	%cell_hrefs = ();

	!exists $_REQUEST {__only_table} or return out_html ({}, $data);

	my $columns  = $_JSON -> encode ($options -> {header} ||= [
	
		map {{
		
			header    => '',
		
			dataIndex => 'f' . $_,
	
		}} (0 .. $n - 1)
	
	]);
	
	my $fields   = $_JSON -> encode (['id', map {{name => $_ -> {dataIndex}}} @{$options -> {header}}]);

	my $storeOptions = $_JSON -> encode ({
		storeId     => "store_$options->{name}",
	});

	my $panelOptions = $_JSON -> encode ({
		anchor     => '100 100%',
		title      => $options -> {title},
		border     => \0,
		viewConfig => {autoFill => \1},
	});
	
	my %base_params = %_REQUEST_VERBATIM;
	
	$base_params {__only_table} = $options -> {name};
	
	my $base_params = $_JSON -> encode (\%base_params);
	
	$_REQUEST {__scrollable_table_row} += 0;
	
	return qq {ui.target.add (createGridPanel($data,$columns,$storeOptions,$fields,$panelOptions,$base_params,$toolbar,$_REQUEST{__scrollable_table_row}));};
	
}

################################################################################

sub draw_toolbar_input_text {

	my ($_SKIN, $options) = @_;

	return 'draw_toolbar_input_text';
	
}

################################################################################

sub draw_toolbar_input_select {

	my ($_SKIN, $options) = @_;

	return 'draw_toolbar_input_select';
	
}

################################################################################

sub draw_toolbar_input_datetime {

	my ($_SKIN, $options) = @_;
	
	$options -> {format} =~ s{\%}{}g;

	return 'draw_toolbar_input_datetime';
	
}

################################################################################

sub draw_toolbar_pager {

	my ($_SKIN, $options) = @_;

	return 'draw_toolbar_pager';
	
}

################################################################################

sub draw_toolbar {

	my ($_SKIN, $options) = @_;

	return $options;

}

################################################################################

sub draw_text_cell {

	my ($_SKIN, $data, $options) = @_;
	
	my $i = ${$_PACKAGE . 'i'};
	
	my $v = ($i -> {__field_values} ||= {
		
		id   => $i -> {id}, 
		
		cnt  => 0,
		
	});

	if ($data -> {href}) {
			
		my $l = length $_REQUEST {__uri_root_common};

		if ($_REQUEST {__uri_root_common} eq substr $data -> {href}, 0, $l) {
		
			$data -> {href} = substr $data -> {href}, $l;
		
		}
			
		push @{$cell_hrefs {$data -> {href}}}, [$i -> {__n}, $v -> {cnt}];

	}

	$v -> {'f' . ($v -> {cnt} ++)} = $data -> {label};

	return 1;
	
}

####################################################################

sub draw_table_header_cell {
	
	my ($_SKIN, $cell) = @_;

	return $cell;

}

####################################################################

sub draw_table_header_row {
	
	my ($_SKIN, $data_cells, $html_cells) = @_;

	return $html_cells;

}

####################################################################

sub draw_table_header {
	
	my ($_SKIN, $raw_rows, $rows) = @_;

	@$rows > 0 or return '[]';

	my @cols = ();
	
	my $n    = 0;
	
	foreach my $i (@{$rows -> [0]}) {
	
		my $col = {
		
			header    => $i -> {label},
			
			dataIndex => 'f' . $n ++,
		
		};
		
		$col -> {width} = $i -> {width} if $i -> {width};
		
		push @cols, $col;
	
	}

	return \@cols;

}

################################################################################

sub draw_form_field_datetime {

	my ($_SKIN, $options, $data) = @_;
		
	$options -> {format} =~ s{\%}{}g;

	return 'draw_form_field_datetime';
	
}

################################################################################

sub draw_form_field_hgroup {

	my ($_SKIN, $options, $data) = @_;
		
	return 'draw_form_field_hgroup';
	
}

################################################################################

sub draw_form_field_select {

	my ($_SKIN, $options, $data) = @_;
		
	return 'draw_form_field_select';
	
}

################################################################################

sub draw_form_field_checkboxes {

	my ($_SKIN, $options, $data) = @_;
	
	return 'draw_form_field_checkboxes';
	
}

################################################################################

sub draw_form_field_string {

	my ($_SKIN, $options, $data) = @_;
	
	delete $options -> {attributes};
	
	return 'draw_form_field_string';
	
}

################################################################################

sub draw_form_field_static {

	my ($_SKIN, $options, $data) = @_;
		
	return 'draw_form_field_static';
	
}

################################################################################

sub draw_form_field {

	my ($_SKIN, $field, $data) = @_;
	
	return 'draw_form_field';

}

################################################################################

sub draw_path {

	my ($_SKIN, $options, $list) = @_;
		
	return $list;
	
}	

################################################################################

sub draw_centered_toolbar_button {

	my ($_SKIN, $options) = @_;
	
	if ($options -> {href} =~ /^javaScript\:/i) {
		
		$options -> {handler} = $';
	
	}
	else {
	
		$options -> {handler} = qq {nope ('$options->{href}', '$options->{target}')};
	
	}
		
	return '';
	
}

################################################################################

sub draw_centered_toolbar {

	my ($_SKIN, $options, $list) = @_;
	
	my @list = ();
	
	foreach my $i (@$list) {
	
		next if $i -> {off};
		
		delete $i -> {$_} foreach qw (href target off preset html preconfirm confirm);

		delete $i -> {hotkey} -> {$_} foreach qw (off type);

		push @list, $i;
	
	}

	return \@list;

}

################################################################################

sub draw_form {

	my ($_SKIN, $options) = @_;
	
	delete $options -> {data};

	return 'ui.target.add (createFormPanel(' . $_JSON -> encode ($options) . '));';

}

################################################################################

sub draw_redirect_page {

	my ($_SKIN, $options) = @_;

	my $target = $options -> {target} ? "'$$options{target}'" : 'null';
	
	if ($options -> {label}) {

		$options -> {before} = 'Ext.MessageBox.alert (' . $_JSON -> encode ($options -> {label}) . ');';

	}
	
	my $js = qq {
	
		$options->{before};
		
		nope ('$options->{url}&_salt=' + Math.random (), $target);
	
	};
	
	return qq {<html><head><script>
	
		var w = window.name == 'invisible' ? window.parent : window;
	
		w.eval (@{[$_JSON -> encode ($js)]});
	
	</script></head></html>}

}

################################################################################

sub draw_error_page {

	my ($_SKIN, $page) = @_;

	$_REQUEST {__content_type} = 'text/html; charset=' . $i18n -> {_charset};

	my $js = qq {Ext.MessageBox.alert ('������', @{[$_JSON -> encode ('<pre>' . $_REQUEST {error} . '</pre>')]});};
	
	$_REQUEST {__iframe_target} or return $js;
	
	$_REQUEST {__content_type} ||= 'text/javascript; charset=' . $i18n -> {_charset};
	
	$data = $_JSON -> encode ($js);
	
	return qq {<html><head><script>
	
		parent.eval (@{[$_JSON -> encode ($js)]});
	
	</script></head></html>}

}

################################################################################

sub draw_logon_form {

	$keepalive = $preconf -> {no_keepalive} ? '' : 'setInterval (sendKeepAliveRequest, ' . (60000 * (($conf -> {session_timeout} ||= 30) - 0.5)) . ');';

	return <<EOS;
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.0 Transitional//EN" 
    "http://www.w3.org/TR/html4/loose.dtd">
<html>
	<head>
		<link rel="stylesheet" type="text/css" href="/i/ext/resources/css/ext-all.css" />
		<script type="text/javascript" src="/i/ext/adapter/ext/ext-base.js"></script>
		<script type="text/javascript" src="/i/ext/ext-all.js"></script>
		<script type="text/javascript" src="/i/ext/src/locale/ext-lang-ru.js"></script>
		<script type="text/javascript" src="/i/_skins/ExtJs/navigation.js"></script>

		<style>
			.ext-ie .x-form-text {
			    margin: 0px;
			}
			.no-icon {
				display : none;
			}
		
		</style>

		<script type="text/javascript">
			
			Ext.Ajax.defaultHeaders = {
				'Content-Type-Charset': '$$i18n{_charset}'
			};
			
			var ui = {
				sid     : @{[ $_JSON -> encode ($_REQUEST {sid}) ]},
				panel   : {},
				hotkeys : {}
			};

			ui.checkMenu = function (md5) {

				if (ui.menu_md5 != md5) ui.refreshSubset (ui.subsetCombo, null, 0);

			}

			ui.refreshSubset = function (combo, record, index) {

				ui.subsetStore.proxy.setUrl ("/?type=menu&action=serialize&sid=" + ui.sid + "&__subset=" + (

					record ? record.data.name : combo.getValue ()

				));

				ui.subsetStore.load ({

					params   : {},
					scope    : ui.subsetStore,
					callback : function () {

						var data = ui.subsetStore.reader.jsonData;

						ui.subsetCombo.setValue (data.user.subset);
												
						createMenu (ui.panel.center.getTopToolbar (), data.__menu, ui.oldSubset != data.user.subset);
						
						ui.fioLabel.setText (data.user.label);

						ui.oldSubset = data.user.subset;

						ui.menu_md5 = data.md5;

						combo.collapse ();

						ui.panel.center.focus ();

					}

				});

			}

			ui.subsetStore = new Ext.data.JsonStore ({

				url        : "/",
				root       : "__subsets",
				fields     : ['name', 'label'],
				idProperty : 'name'

			});


			ui.subsetCombo = new Ext.form.ComboBox ({

				editable         : false,
				forceSelection   : true,

				displayField     : 'label',
				valueField       : 'name',
				mode			 : 'local',

				fieldLabel       : '������-�������',

				disableKeyFilter : false,
				triggerAction    : 'all',

				listeners        : {

					select : ui.refreshSubset

				},

				store: ui.subsetStore

			});

			ui.panel.north = new Ext.form.FormPanel ({

				frame:true,
				bodyStyle:'padding:1px 1px 0',
				layout: 'hbox',
				layoutConfig  : {
					align: 'middle',
					defaultMargins  : {top:0, right:20, bottom:0, left:0}
				},
				region: 'north',
				split: true,
				header: false,
				height: 80,
				collapsible: true,
				margins: '0 0 0 0'

			});

			ui.exitButton = new Ext.Button ({

				text       : '�����',
				icon       : '/i/ext/examples/shared/icons/fam/user_delete.png',
				iconAlign  : 'right',
				scale      : 'medium',
				listeners  : {click : applicationExit}

			});
			
			ui.fioLabel = new Ext.form.Label ({
				text : fio
			});

			ui.panel.north.add ([

				new Ext.form.Label ({html : '&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<img src="/i/logo_in.gif"></img>'}),
				new Ext.form.Label ({text : '������-�������: '}),
				ui.subsetCombo,
				ui.fioLabel,
				new Ext.form.Label ({text : ' ', flex : 1}),
				ui.exitButton

			]);

			ui.panel.center = new Ext.form.FormPanel ({
				tbar   : {},
				region : 'center',
				id     : 'center',
				border : false,
				layout : 'fit'
			});
			
			ui.viewportOptions = {			
				layout: 'border',
				items: [ui.panel.north, ui.panel.center]
			}
			
			ui.init = function () {

				ui.viewport = new Ext.Viewport (ui.viewportOptions);

				if (ui.loginForm) ui.loginForm.close ();

				ui.checkMenu (-1);
				
				return null;

			}

			Ext.onReady (function () {
			
				$keepalive
			
				Ext.get (document.body).on ('keydown', bodyOnKeyDown);

				if (ui.sid) return ui.init ();
				
				var loginFormPanel =  new Ext.FormPanel ({ 

					labelWidth   : 80,
					url          : '/', 
					frame        : true, 
					title        : '$i18n->{authorization}', 
					defaultType  : 'textfield',
					monitorValid : true,

					items:[
						{ 
							name:'type', 
							inputType:'hidden', 
							value:'logon'
						},
						{ 
							name:'action', 
							inputType:'hidden', 
							value:'execute'
						},
						{ 
							name:'__iframe_target', 
							inputType:'hidden', 
							value:'invisible'
						},
						{ 
							fieldLabel:'$i18n->{login}', 
							name:'login', 
							allowBlank:false 
						},
						{ 
							fieldLabel:'$i18n->{password}', 
							name:'password', 
							inputType:'password', 
							allowBlank:false 
						}
					],

					buttons: [

						{ 
							text:'$i18n->{execute_logon}',
							formBind: true,
							handler:function () { 
								var f = loginFormPanel.getForm ().getEl ().dom;
								f.target = 'invisible';
								f.submit ();
							} 
						}

					] 

				});

				ui.loginForm = new Ext.Window ({

					layout    :'fit',
					width     : 300,
					height    : 150,
					closable  : false,
					resizable : false,
					plain     : true,
					border    : false,
					items     : [loginFormPanel]

				});
	
				ui.loginForm.show ();
				
			});

		</script>

	</head>

	<body>
		<iframe name=invisible src="about:blank" width=0 height=0 application="yes">
		</iframe>
	</body>

</html>
EOS

}

1;