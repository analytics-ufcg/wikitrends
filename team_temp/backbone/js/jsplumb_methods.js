var instance;


function init_jsplumb(){

	    jsPlumb.ready(function() {
		
			instance = jsPlumb.getInstance({
				Endpoint : ["Dot", {radius:0.1}],//sem bolinha
				HoverPaintStyle : {strokeStyle:"#1e8151", lineWidth:2 },
				
				Container:"statemachine-demo"
			});

		});

		var windows = jsPlumb.getSelector(".statemachine-demo .w");
		//console.log(windows);
			
		instance.draggable(windows);
	   
		instance.doWhileSuspended(function() {
										
			instance.makeSource(windows, {
				filter:".ep",				// only supported by jquery
				anchor:"Continuous",
				connector:[ "StateMachine", { curviness:1 } ],
				
				
			});						

			// initialise all '.w' elements as connection targets.
	        instance.makeTarget(windows, {
				dropOptions:{ hoverClass:"dragHover" },
				anchor:"Continuous"				
			});
			
		});
}


function jsplumb_connection(source, target){
	
	instance.connect(
		{ 
			source:source+"", 
			target:target+"", 
			newConnection:false,
			paintStyle : {
					strokeStyle : "#5c96bc",
					lineWidth : 1,
					outlineColor : "transparent",
					outlineWidth : 4
			},
			overlays: [
						["Arrow", 
							{
							location:1,
							id: "arrow",
							length:8,
							foldback:0.7					
							}
						],
						["Label",
							{
							id:"label",
							cssClass:"aLabel"	
							}
						]
			]
		
		});
}


function jsplumbdeleteEveryEndpoint(){
	instance.deleteEveryEndpoint();
}

function jsplumb_CorrelationConnection(source, target, correlacao, correlacaoMin){

	
	var color = "black";

	if (correlacao > 0) {
		color = "green";
	} else {
		color = "red";
	}

	var valorMin = 1;
	var valorMax = 10;

	//Proporção para pegar tamanho da linha. Se correlacao for abs(1) o tamanho da linha vai ser 10.
	//Se a correlacao for abs(0.7) o tamanho da linha vai ser 1.

	var lineWidth = (valorMax * (1 - correlacaoMin) - ((valorMax - valorMin) * (1 - Math.abs(correlacao)) ) ) / (1 - correlacaoMin);


	var c = instance.connect({
		source : source+"",
		target : target+"",
		paintStyle : {
			strokeStyle : color,
			lineWidth : lineWidth,
			outlineColor : "transparent",
			outlineWidth : 4
		}
	});

	c.removeOverlays();

}


function initModal() {

		$('a[name=modal]').click(function(e) {
			e.preventDefault();

			var id = $(this).attr('href');

			var maskHeight = $(document).height();
			var maskWidth = $(window).width();

			$('#mask').css({
				'width' : maskWidth,
				'height' : maskHeight
			});

			$('#mask').fadeIn(400);
			$('#mask').fadeTo("slow", 0.8);

			//Get the window height and width
			var winH = $(window).height();
			var winW = $(window).width();

			$(id).css('top', winH / 2 - $(id).height() / 2);
			$(id).css('left', winW / 2 - $(id).width() / 2);

			$(id).fadeIn(2000);
		});

		$('.window .close').click(function(e) {
			e.preventDefault();

			$('#mask').hide();
			$('.window').hide();
		});

		$('#mask').click(function() {
			$(this).hide();
			$('.window').hide();
		});
}