#include "inspector.h"

docdb::Inspector::FileMap docdb::Inspector::fileMap={
		{"index.html",{
				"text/html;charset=utf-8",
R"html(<!DOCTYPE html>
<html>
<head>
<title>DocDB Inspector</title>
<meta charset="utf-8"/>
<link rel="stylesheet" href="style.css" />
</head>
<body onload="start()">
<div class="directory">
<dl>
<dt>root</dt>
<dd><a href="#class=&name=">stats</a></dd>
</dl>
<div id="directory">
</div>
</div>
<div id="content">
<div id="header">
<div id="info">
<h1><span id="classname">docdb</span>:<span id="objname">testdb</span></h1>
<table>
<tr><th>keyspaceID:</th><td id="info_kid"></td></tr>
<tr><th>size:</th><td id="info_size"></td></tr>
</table>
</div>
<div id="nav">
<button id="reload">Reload</button>
<select id="operation">
<option value="from">>=</option>
<option value="key">=</option>
<option value="prefix_str">"%"</option>
<option value="prefix">[%]</option>
</select>
<input type="search" id="search">
<button id="prev">prev</button>
<button id="next">next</button>
<select id="limit">
<option>10</option>
<option>30</option>
<option>50</option>
<option>100</option>
<option>200</option>
<option>500</option>
<option>1000</option>
</select>
<span>Offset:</span><input type="number" id="offset" step="1">
<select id="format">
<option value="norm">Table</option>
<option value="raw">Raw</option>
</select>

</div>
</div>
<div id="result">
</div>
</div>

<script type="text/javascript" src="code.js"></script>
</body>
</html>)html"}},
		{"style.css",{
				"text/css;charset=utf-8",
R"css(body {
	margin:0;
	padding:0;
	min-height: 100vh;
}

.directory {
    height: 100%;
    position: fixed;
    width: 200px;
    box-sizing: border-box;
    padding: 5px;
    background: linear-gradient(-120deg,#e9e9e9, white);
    overflow: auto;
}


#content {
	margin-left: 201px;
	
}

#nav {
	display: flex;	
	background-color: #e9e9e9
}
#nav[hidden] {
	display:none;
}
#nav > * {
	margin: 1px;

}

#offset {
	width: 4em;
}
#search {
	flex-grow:1;
}

#info {
	padding: 10px;
	display: flex;
	justify-content: space-between;
    background: linear-gradient(0deg,#e9e9e9, white);
}

#info h1 {
	margin: 0;

}

#info table th {
	text-align:right;
}

.directory dl {
	margin: 0;
}

.directory dl dt {
	margin-top: 1em;
	font-weight: bold;
	background: linear-gradient(#d5d4d4c4,#ffffffcf);
	padding: 2px;
}

.directory dl dd {
	margin:0;
}

.directory dl dd a {
	display: block;
	padding: 2px 2px 2px 30px;
	color: black;
	text-decoration: auto;
}
.directory dl dd a:hover {
	background: #d1d0d0;
}

#result {
	margin: 5px;
}

#result table {
	width: 100%;
	border: 1px solid;
	border-collapse:collapse;
}

#result table th {
    background-color: #CCC;
    padding: 5px;
    border-right: 1px dotted;
}

#result table th.separator {
	   border-right: 2px solid;
}

#result table td {
    padding: 2px 5px;
    border-right: 1px dotted;
    border-bottom: 1px dotted;
}

#result table td.separator {
	   border-right: 2px solid;
}

#result td {
	/* width: 1%; */
}


#result td.number {
	text-align: right;
	color: blue;	
}

#result td.string {
	text-align: left;
	color: #106f10;	
}


#result td.object {
	font-family: monospace;
}

#result td.boolean, #result td.null_value{
	font-family: monospace;
	text-align: center;
	font-weight: bold;

}
#result td.undefined_value{
	font-family: monospace;
	text-align: center;
	color: red;

}

#result tr:hover td {
	background-color: #EEE;
}

#header {
	position: sticky;
	top: 0;
}

.stats {
    display: flex;
}

.stats > div {
	border-top: 1px solid;
	flex-grow: 1;
	margin: 1em;
	width: 100px;
}

.stats > div > div {
   padding: 10px;
   box-sizing: border-box;
   position: relative;
   border-bottom: 1px solid;
}

.stats > div > div > div {
	position: absolute;
	left:0;
	top:0;
	bottom:0;
	background-color: #0000FF20;
}

.stats > div > button {
	display:  block;
	width: 10em;
	height: 3em;
	margin: 1em auto;
})css"}},
{"code.js",{"text/javascript",R"javascript("use strict"

var path = ".";

var title;
var prevHash;

var post_enabled;
var compact_running;

function start() {

	window.onhashchange = updateAfterHashChange;
	updateAfterHashChange();
	title = document.title;

	document.getElementById("operation").addEventListener("change", applyChange);
	document.getElementById("format").addEventListener("change", applyChange);
	document.getElementById("limit").addEventListener("change", applyChange);
	document.getElementById("offset").addEventListener("input", applyChangeDelayed);
	document.getElementById("search").addEventListener("input", applyChangeDelayed);
	document.getElementById("next").addEventListener("click", goNext.bind(null,1));
	document.getElementById("prev").addEventListener("click", goNext.bind(null,-1));
	document.getElementById("reload").addEventListener("click", goNext.bind(null,0));

	post_enabled=fetch(path+"/test",{method:"POST"}).then(st=>{		
		return st.status==200;
	});
	compact_running=Promise.resolve(true);

}

function replaceContent(id, content) {
	var el = document.getElementById(id);
	while (el.firstChild) {
		el.removeChild(el.firstChild);
	}
	if (content) el.appendChild(content);
}

var curClass;
var curName;

const stateFields = ["operation","search","limit","offset","format"];

function createState() {
	var state = {};
	stateFields.forEach(id=>{
		var el = document.getElementById(id);
		state[id] = el.value;
	});
	state.class = curClass;
	state.name = curName;
	return state;
}

function stateToHash(state) {
	var items = [];
	for (var x in state) {
		items.push(x+"="+encodeURIComponent(state[x]));
	} 
	return "#"+items.join("&");
}

function hashToState(hash) {
	var st = createState();
	if (hash.length) {
		if (hash[0] == '#') hash = hash.substr(1);
		hash.split("&").forEach(itm=>{
			var p = itm.split("=");
			var v = decodeURIComponent(p[1]);
			st[p[0]] = v;
		});		
	} 
	return st;
}

var redraw_directory;

function update_directory() {
	return fetch(path+"/db/")
		.then(x=>x.json())
		.then(dbinfo=>{
			var data = dbinfo.keyspaces;
			var st = createState();
			redraw_directory = function(st){			
				var struct = {};
				data.forEach(row=>{
					var cls=row.class;
					var itm = {id:row.id, name:row.name};
					if (!struct[cls]) struct[cls]=[];
					struct[cls].push(itm);			
				});
				
				var dl = document.createElement("dl");
				
				for (var x in struct) {
					var itms = struct[x];
					var dt = document.createElement("dt");
					dt.textContent = x;
					dl.appendChild(dt);
					itms.forEach(itm=>{
						var dd = document.createElement("dd");
						var a = document.createElement("a");
						dd.appendChild(a);
						dl.appendChild(dd);
						st.class = x;
						st.name = itm.name;
						st.offset = 0;
						a.textContent = itm.name;
						a.href=stateToHash(st);					
					});				
				}
				replaceContent("directory",dl);
			};
			redraw_directory(st);		
			return  dbinfo;
		});
}

function update_info_indirect(data) {
			document.getElementById("info_kid").textContent = data.kid;
			var sz;
			if (data.size == 0) sz = "<100 KiB";  
			else if (data.size > 1024*1024) sz = (data.size/(1024*1024)).toFixed(2)+" MiB";
			else sz = (data.size/1024).toFixed(2)+" KiB";
			document.getElementById("info_kid").textContent = data.kid;
			document.getElementById("info_size").textContent = sz;	
}

function update_info(st) {
	var uri = path+"/db/"+encodeURIComponent(st.class)+"/"+encodeURIComponent(st.name)+"/info";
	fetch(uri).then(x=>x.json())
		.then(data=>{
			update_info_indirect(data);
		});
}

function setHash(st) {
	var h = stateToHash(st);
	prevHash = h;
	location.hash = h;	
}

function updateAfterHashChange() {
	var h = location.hash;
	if (prevHash == h) return;
	prevHash = h; 
	var st = hashToState(h);
	stateFields.forEach(id=>{
		if (st[id]) 
			document.getElementById(id).value=st[id];
	});
	if (st.class) curClass = st.class;
	if (st.name) curName = st.name;
	document.getElementById("classname").textContent=st.class;
	document.getElementById("objname").textContent=st.name;
	var stats = update_directory();
	if (st.class) {
	    document.getElementById("nav").hidden=false;
		update_result(st);
	    update_info(st);
	} else {
		document.getElementById("nav").hidden=true;
		show_stats(stats);		
	}
}

function createTD(val) {
	var td = document.createElement("td");
	if (val === "undefined") {
		td.setAttribute("class","undefined_value");
		td.textContent="undefined";		
	} else if (val === null) {
		td.setAttribute("class","null_value");
		td.textContent="null";
	} else if (typeof val == "object") {
		td.textContent = JSON.stringify(val);
		td.setAttribute("class","object");
	} else if (typeof val == "number") {
		td.textContent = val
		td.setAttribute("class","number");
	} else if (typeof val == "boolean") {
		td.textContent = val?"true":"false";
		td.setAttribute("class","boolean");
	} else {
		td.textContent = val;
		td.setAttribute("class","string");
	}
	return td;

}

function update_result(st) {
	if (curName === undefined) {
        replaceContent("result",null);
		return;
	}
	var uri = path+"/db/"+encodeURIComponent(curClass)+"/"+encodeURIComponent(curName);
	var query = {}; 
	var srch;
	try {
		srch = JSON.parse(st.search);
	} catch (e) {
		srch = st.search;
	}	
	
	if (st.operation == "key") {
		query.key = JSON.stringify(srch);
	} else if (st.operation == "prefix")  {
		if (!Array.isArray(srch)) srch = [srch];
		query.prefix = JSON.stringify(srch);
	} else if (st.operation == "prefix_str") {
		query.prefix = JSON.stringify(st.search);
	} else if (st.operation == "from") {
		query.start_key = JSON.stringify(srch);
	}
	
	query.offset = st.offset;
	query.limit = st.limit;
	if (st.format == "raw") query.raw = 1;
	uri = uri + "?" + stateToHash(query).substr(1);
	fetch(uri).then(x=>x.json())
		.then(data=>{
			var collumns = 1;
			var has_id = false;
			data.forEach(x=>{
				if (Array.isArray(x.key)) {
				   if (x.key.length>collumns) collumns = x.key.length;
				}
				if (x.id !== undefined) {
					has_id = true;
				}
			});
				
			var table = document.createElement("table");
			var tr = document.createElement("tr");
			var th = document.createElement("th");
			th.textContent = "Key";
			tr.appendChild(th);
			table.appendChild(tr);
			for (var i = 1; i < collumns; i++) {
				th = document.createElement("th");
				th.textContent=i+1;
				tr.appendChild(th);
			}
			th.classList.toggle("separator", true);
			th = document.createElement("th");
			th.textContent="Value";
			tr.appendChild(th);
			if (has_id) {
				th = document.createElement("th");
				th.textContent="ID";
				tr.appendChild(th);
			}
			
            data.forEach(row=>{
                tr = document.createElement("tr");
               	var td;
                if (Array.isArray(row.key)) {
                    row.key.forEach(c=>{
                        td = createTD(c);
                        tr.appendChild(td);
                    });
                    var span = collumns - row.key.length;
                    if (span) td.setAttribute("colspan",span+1);
                    td.classList.toggle("separator", true);
                } else {
                    td = createTD(row.key);
                    if (collumns>1) td.setAttribute("colspan",collumns);
                    td.classList.toggle("separator", true);
                    tr.appendChild(td);
                }
               td = createTD(row.value);
               tr.appendChild(td);
               if (has_id) {
				   td = createTD(row.id);
				   tr.appendChild(td);               	
               }
               table.appendChild(tr);
            });

			replaceContent("result", table);

		});	 
}

var tm;

function applyChange() {
	var st = createState();
	update_result(st);
	redraw_directory(st);
	setHash(st);
	tm = undefined;	
}



function applyChangeDelayed() {
	if (tm) clearTimeout(tm);
	tm = setTimeout(applyChange, 500);	
}

function goNext(m) {
	var st = createState();
	st.offset = parseInt(st.offset)+parseInt(st.limit) * m;
	if (st.offset < 0) st.offset = 0;
	document.getElementById("offset").value = st.offset;
	update_result(st);
	redraw_directory(st);
	setHash(st);
}

async function show_stats(stats_promise) {
	var stats=await stats_promise;

    var screen = document.createElement("div");
    screen.setAttribute("class","stats");
    var tbl1 = document.createElement("div");
    var kspc = stats.keyspaces;
    var lvst = stats.stats.levels
    kspc.sort((a,b)=>b.size-a.size);
    var maxsz = kspc.reduce((a,b)=>a>b.size?a:b.size,1);
    var totalsz = kspc.reduce((a,b)=>a+b.size,0);
    kspc.forEach(row=>{
    	var div = document.createElement("div");
    	var div2 = document.createElement("div");
    	div.appendChild(div2);
    	var text = document.createTextNode(row.class+":"+row.name);
    	div.appendChild(text);
    	div2.style.width = (100*row.size/maxsz)+"%";
    	tbl1.appendChild(div);
    });
    screen.appendChild(tbl1);


    var tbl2 = document.createElement("div");
    var maxlevelsz = lvst.reduce((a,b)=>a>b.size_mb?a:b.size_mb,1);

    lvst.forEach(row=>{
    	var div = document.createElement("div");
    	var div2 = document.createElement("div");
    	div.appendChild(div2);
    	var caption = "Level "+row.level+" (size: "+row.size_mb+" MB / files: "+row.files+")";
    	var text = document.createTextNode(caption);
    	div.appendChild(text);
    	div2.style.width = (100*row.size_mb/maxlevelsz)+"%";
    	tbl2.appendChild(div);    	
    });

    (function(){
    	var div = document.createElement("div");
    	var caption = "Memory: "+stats.stats.memory_usage;
    	var text = document.createTextNode(caption);
    	div.appendChild(text);
    	tbl2.appendChild(div);   
    })();

    screen.appendChild(tbl2);
    post_enabled.then(x=>{if (x) {
		var bt = document.createElement("button");
		bt.textContent="Compact";
		bt.disabled = true;
		bt.addEventListener("click",function(){    
		    bt.disabled = true;
            compact_running = fetch(path+"/compact",{method:"POST"})
                .then(function(){show_stats(update_directory());});
		});
		compact_running.then(()=>{bt.disabled=false;});
		tbl2.appendChild(bt);
    }});

    replaceContent("result", screen);
    update_info_indirect({"size":totalsz,kid:"n/a"});
	document.getElementById("classname").textContent="root";
	document.getElementById("objname").textContent="stats";

}
)javascript"}}


};

