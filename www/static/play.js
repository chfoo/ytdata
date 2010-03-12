
var vids_per_page = 1000;
var page = null;
var index = null;
var video_num = null;
var ids = null;
var ytplayer = null;
var running = false;

$(document).ready(function(){
	main();
})

function main() {
	if (location.hash != "") {
		video_num = parseInt(location.hash.substring(1));
//		alert(video_num);
		page = Math.floor(video_num / vids_per_page);
		index = video_num % vids_per_page;
	} else {
		video_num = 0;
		page = 0;
		index = 0;
	}
	
	get_ids(page);
	prepare_content();
}

function get_ids(page) {
	var play_js = this;
$.getJSON(
	"browse.cgi?bot&page=0&callback=?",
	function(data) {
		play_js.ids = eval(data);
	}
	);
}


function prepare_content() {
	$("<div id=\"vidTitle\"></div><div id=\"swfContainer\"><div id=\"swfPlaceholder\"></div></div>").appendTo("#mainContent");
	var params = { allowScriptAccess: "always" };
	var atts = { id: "myytplayer" };
	swfobject.embedSWF("http://www.youtube.com/v/jNQXAC9IVRw?enablejsapi=1&playerapiid=ytplayer&rel=0&fs=1", 
"swfPlaceholder", "720", "400", "8", null, null, params, atts);

}

function onYouTubePlayerReady(playerId) {
	ytplayer = document.getElementById("myytplayer");
	ytplayer.addEventListener("onStateChange", "player_state_changed_cb");
	ytplayer.addEventListener("onError", "player_on_error_cb");
	
	running = true;
	play();
}

function play() {
	location.hash = video_num;
//	alert(ids);
//	alert("going to play"+ids[index]);
	if (ids[index] == null) {
		$("#vidTitle").text("Loading...");
		setTimeout("play()", 1000);
	} else {
	$("#vidTitle").text("#" + video_num + " " + ids[index]);
//		alert(ids[index]);
//	ytplayer.clearVideo();
	//ytplayer.cueVideoById(""+ids[index]);
	ytplayer.playVideo();
	//ytplayer.loadVideoById(ids[index]);
//	setTimeout("next();play();", 1);
	}
}

function player_state_changed_cb(state) {
	if (state == 0 && running) {
		next();
		play();
	}
}

function player_on_error_cb(code) {
	next();
	play();
}

function next() {
	index += 1;
	video_num += 1;
	$("#vidTitle").text("Next: #" + video_num + "  Page: "+ page );
	if (index > ids.length || ids[index] == null) {
		page += 1;
		index = 0;
		get_ids(page);
	//	alert("next page");
	}
}
