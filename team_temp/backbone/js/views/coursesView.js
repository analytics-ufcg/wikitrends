// Person Model
// View for all people
directory.CoursesView = Backbone.View.extend({

	tagName : 'div',
	className : "demo statemachine-demo",
	id : "statemachine-demo",

	initialize : function() {

		var dataframe = readJSON("http://analytics.lsd.ufcg.edu.br/ccc/disciplinasPorPeriodo");
		console.log(dataframe);
		console.log(decodeURIComponent(escape("Avalia\u001a\u001ao")));
		this.collection = new directory.CourseCollection(dataframe);

	},

	render : function() {

		this.collection.each(function(course) {

			var courseView = new directory.CourseView({
				model : course
			});
			this.$el.append(courseView.render().el);

		}, this);

		return this;
	},

	connect : function(url) {
		$(".w").unbind('mouseover mouseout');

		var dataframe = readJSON(url);

		///////////////////Cadastrar evento de Mouseover///////////////////
		hashPreRequesitos = {}
		instance.setSuspendDrawing(true);

		_.each(dataframe, function(data) {
			codigoPreRequisito = data["codigoPreRequisito"];
			codigo = data["codigo"];
			//Cria conexões
			//if (vaiGerar) {
			jsplumb_connection(codigoPreRequisito, codigo);
			//}
			adicionaValorHash(hashPreRequesitos, codigoPreRequisito, codigo);
			adicionaValorHash(hashPreRequesitos, codigo, codigoPreRequisito);

		});

		//Loop para criar evento que pinta caixinhas correlacionadas ao passar mouseover
		 _.each(Object.keys(hashPreRequesitos), function(data) {

		 	mouseOverPrerequeisitos(data, hashPreRequesitos[data]);

		 });
		 //////////////////////////////////////////////////////////////////

		instance.setSuspendDrawing(false, true);

		 
/*		if (vaiGerar) {
			instance.setSuspendDrawing(true);

			_.each(dataframe, function(data) {
				codigoPreRequisito = data["codigoPreRequisito"];
				codigo = data["codigo"];
				jsplumb_connection(codigoPreRequisito, codigo);

			});


			instance.setSuspendDrawing(false, true);
		}*/

	},

	setPositions : function(url, change_opacity) {

		var dataframe = readJSON(url);

		var top_div = new Array(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

		refreshSlots();
		//
		$(".w").unbind('mouseover mouseout');

		_.each(dataframe, function(data) {

			var periodo;
			var topMin = 1;

			if (change_opacity == 1) {
				topMin = 100;
				periodo = data["periodoMaisFreq1st"];
			} else
				periodo = data["periodo"];

			var slot = $("#" + data["codigo"]);

			slot.css('top', 100 * top_div[periodo] + topMin + 'px');

			top_div[periodo]++;

			slot.css('left', getSlotCaixa() * (periodo - 1) + 'px');

			var frequencia1 = data["freqRelativa1st"];
			var total_de_alunos_da_disciplina = data["totalDeAlunos"];

			//Resetar tooltip
			resetTooltip(slot);

			//if para saber se é blocagem comum
			if (change_opacity == 1) {

				coloracaoDaBlocagemMaisComum(slot, frequencia1);
				slot.attr('title', "Frequência de alunos neste período: " + (frequencia1 * 100).toString().substr(0, 4) + "%" + "\nTotal de alunos: " + total_de_alunos_da_disciplina);

				slot.mouseover(function() {
					setDivMouseOn(slot, data["periodoMaisFreq2nd"], data["periodoMaisFreq3rd"], data["freqRelativa2nd"], data["freqRelativa3rd"], slot.text());
					$("#blocagem1").show();
					$("#blocagem2").show();
				});

			} else {
				slot.css({
					"opacity" : 0.9
				});
			}
			slot.show();

		});

	},

	setPositions2 : function(url, change_opacity) {

		var dataframe = readJSON(url);

		var top_div = new Array(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

		refreshSlots();
		//
		$(".w").unbind('mouseover mouseout');

		var per;

		_.each(dataframe, function(data) {

			var periodo;
			if (change_opacity == 1) {
				periodo = data["periodoMaisFreq1st"];
			} else{
				periodo = data["periodo"];
			}
			per = periodo;
		});

		_.each(dataframe, function(data) {

			var periodo;
			var topMin = 1;

			if (change_opacity == 1) {
				topMin = 12;
				periodo = data["periodoMaisFreq1st"];
			} else{
				periodo = data["periodo"];
			}

			var slot = $("#" + data["codigo"]);

			slot.css('top', 12 * top_div[periodo] + topMin + '%');

			top_div[periodo]++;

			slot.css('left', (95/per) * (periodo - 1) + '%');

			var frequencia1 = data["freqRelativa1st"];
			var total_de_alunos_da_disciplina = data["totalDeAlunos"];

			//Resetar tooltip
			resetTooltip(slot);

			//if para saber se é blocagem comum
			if (change_opacity == 1) {

				coloracaoDaBlocagemMaisComum(slot, frequencia1);
				slot.attr('title', "Frequência de alunos neste período: " + (frequencia1 * 100).toString().substr(0, 4) + "%" + "\nTotal de alunos: " + total_de_alunos_da_disciplina);

				slot.mouseover(function() {
					setDivMouseOn2(slot, data["periodoMaisFreq2nd"], data["periodoMaisFreq3rd"], data["freqRelativa2nd"], data["freqRelativa3rd"], (95/per),slot.text());
					$("#blocagem1").show();
					$("#blocagem2").show();
				});

			} else {
				slot.css({
					"opacity" : 0.9
				});
			}
			slot.show();

		});

	},

	taxaReprovacao : function(url) {

		var dataframe = readJSON(url);

		//var img = document.createElement("IMG");
		//img.src = "image/legenda.jpeg";
		//document.getElementById('legenda').appendChild(img);

		$("#blocagem1").css("background-color", "red");
		$("#blocagem2").css("background-color", "red");
		$(".w").css("background-color", "red");
		$(".w").css("opacity", 1);

		_.each(dataframe, function(data) {

			var frequencia_absoluta = data["reprovacaoAbsoluta"]
			var media_de_reprovacoes = data["reprovacaoRelativa"];
			var slot = $("#" + data["codigo"]);

			var total_de_alunos = data["totalDeAlunos"];

			if (media_de_reprovacoes >= 0 && media_de_reprovacoes <= 0.1) {
				slot.css("background-color", "#FF4D94");
			}
			if (media_de_reprovacoes > 0.1 && media_de_reprovacoes <= 0.3) {
				slot.css("background-color", "#B23668");
			}
			if (media_de_reprovacoes > 0.3 && media_de_reprovacoes <= 0.5) {
				slot.css("background-color", "#8E2B53");
			}
			if (media_de_reprovacoes > 0.5 && media_de_reprovacoes <= 0.7) {
				slot.css("background-color", "#722242");
			}
			if (media_de_reprovacoes > 0.7) {
				slot.css("background-color", "#441428");
			}
			slot.attr('title', "Porcentagem de sucesso: " + ((1 - media_de_reprovacoes) * 100).toString().substr(0, 4) + "%" + "\nTotal de alunos que cursaram: " + Math.round(total_de_alunos));

		});
	},

	correlacao : function(url) {
		$(".w").unbind('mouseover mouseout');

		var dataframe = readJSON(url);
		var min = 1;

		min = _.min(dataframe, function(data) {
			return data["correlacao"];
		});

		jsplumbdeleteEveryEndpoint();

		instance.setSuspendDrawing(true);

		//Tabela hash, onde a chave é o código da disciplina e o conteúdo é um array contendo todas as disciplinas
		//que são correlacionadas com ela.
		var hash = {};

		_.each(dataframe, function(data) {
			var codigo1 = data["codigo1"];
			var codigo2 = data["codigo2"];

			//Adiciona disciplina correlacinada na hash
			adicionaValorHash(hash, codigo1, codigo2);
			adicionaValorHash(hash, codigo2, codigo1);

			jsplumb_CorrelationConnection(codigo2, codigo1, data["correlacao"], min["correlacao"]);

		});

		//Loop para criar evento que pinta caixinhas correlacionadas ao passar mouseover
		_.each(Object.keys(hash), function(data) {

			mouseOverCorrelacao(data, hash[data]);

		});

		instance.setSuspendDrawing(false, true);
	}
});

var top_div = new Array(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

// The View for a CourseView
directory.CourseView = Backbone.View.extend({

	template : _.template("<div class='w' id='<%= codigo %>'><%= disciplina %><div class='ep'></div></div>"),

	render : function() {
		var dataframe = this.model.toJSON();
		var periodo = dataframe["periodo"];

		this.el = $(this.template(dataframe))
		this.el.css('top', 100 * top_div[periodo] + 1 + 'px');
		top_div[periodo]++;
		this.el.css('left', getSlotCaixa() * (periodo - 1) + 'px');
		return this;
	}
});

//Função que adiciona um valor num array de uma key de um hash
function adicionaValorHash(hash, key, value) {
	if (hash[key] == undefined) {
		hash[key] = [value];
	} else {
		hash[key].push(value);
	}

}

//Função que cadastra mouseover e mouseout na tela de correlação (pinta caixinhas de disciplinas correlacionadas)
function mouseOverCorrelacao(codigo, codigoRelacionadas) {

	var slot1 = $("#" + codigo);

	slot1.mouseover(function() {
		//Cor anterior da caixinha que foi passada pelo mouse. Será preciso para voltar a esta cor no moseout
		var corAnterior1 = slot1.css("background-color");

		//Para cada caixa de disciplina correlacionada, cria evento do mouseout que pinta para a cor atual
		//e depois muda a cor
		_.each(codigoRelacionadas, function(codigo2) {
			var slot2 = $("#" + codigo2);
			var corAnterior2 = slot2.css("background-color");
			slot1.mouseout(function() {
				slot2.css("background-color", corAnterior2);
			});
			slot2.css("background-color", "#0570b0");

		})
		//Muda a cor da caixa que o mouse passou por cima
		slot1.css("background-color", "#0570b0");
		slot1.mouseout(function() {
			slot1.css("background-color", corAnterior1);
		});
	});

}

function mouseOverPrerequeisitos(codigo, codigoRelacionadas) {

	var slot1 = $("#" + codigo);

	slot1.mouseover(function() {
		//Cor anterior da caixinha que foi passada pelo mouse. Será preciso para voltar a esta cor no moseout
		var corAnterior1 = slot1.css("background-color");

		//Para cada caixa de disciplina correlacionada, cria evento do mouseout que pinta para a cor atual
		//e depois muda a cor
		_.each(codigoRelacionadas, function(codigo2) {
			var slot2 = $("#" + codigo2);
			var corAnterior2 = slot2.css("background-color");
			slot1.mouseout(function() {
				slot2.css("background-color", corAnterior2);
			});
			slot2.css("background-color", "#0570b0");

		})
		//Muda a cor da caixa que o mouse passou por cima
		slot1.css("background-color", "#0570b0");
		slot1.mouseout(function() {
			slot1.css("background-color", corAnterior1);
		});
	});

}

function resetTooltip(slot) {
	slot.attr('title', "");
}

function getSlotCaixa() {
	var slot_caixa = (window.screen.availWidth / 10);
	espaco_x = slot_caixa - 100;

	if (espaco_x < 30) {
		return 150;
	}

	return slot_caixa;

}


function setDivMouseOn(slot, PMFreq1, PMFreq2, freqR1, freqR2, nomeCadeira) {
	//console.log(freqR1+" "+freqR2)

	slot.mouseout(function() {
		$("#blocagem1").hide();
		$("#blocagem2").hide();
	});

	slot.mouseover(function() {

		$("#blocagem1").show();
		$("#blocagem2").show();

	});

	$("#blocagem1").text(nomeCadeira);
	$("#blocagem2").text(nomeCadeira);

	coloracaoDaBlocagemMaisComum($("#blocagem1"), freqR1);
	coloracaoDaBlocagemMaisComum($("#blocagem2"), freqR2);

	$("#blocagem1").css("left", getSlotCaixa() * (PMFreq1 - 1));
	$("#blocagem2").css("left", getSlotCaixa() * (PMFreq2 - 1));

}

function setDivMouseOn2(slot, PMFreq1, PMFreq2, freqR1, freqR2, espacamentoEsquerda,nomeCadeira) {
	//console.log(freqR1+" "+freqR2)

	slot.mouseout(function() {
		$("#blocagem1").hide();
		$("#blocagem2").hide();
	});

	slot.mouseover(function() {

		$("#blocagem1").show();
		$("#blocagem2").show();

	});

	$("#blocagem1").text(nomeCadeira);
	$("#blocagem2").text(nomeCadeira);

	coloracaoDaBlocagemMaisComum($("#blocagem1"), freqR1);
	coloracaoDaBlocagemMaisComum($("#blocagem2"), freqR2);

	$("#blocagem1").css('left', espacamentoEsquerda * (PMFreq1 - 1) + '%');
	$("#blocagem2").css('left', espacamentoEsquerda * (PMFreq2 - 1) + '%');

}

function coloracaoDaBlocagemMaisComum(slot, frequencia) {
	if (frequencia >= 0 && frequencia <= 0.2) {
		slot.css("background-color", "#7faaca");
	} else if (frequencia > 0.2 && frequencia <= 0.35) {
		slot.css("background-color", "#3277aa");
	} else if (frequencia > 0.35 && frequencia <= 0.5) {
		slot.css("background-color", "#004d86");
	} else if (frequencia > 0.5 && frequencia <= 0.65) {
		slot.css("background-color", "#003359");
	} else if (frequencia > 0.65 && frequencia <= 1.0) {
		slot.css("background-color", "#00192c");
	}
}

function refreshSlots() {

	jsplumbdeleteEveryEndpoint();

	$('.w').each(function() {
		$(this).hide();

	});
	$("#blocagem1").css("background-color", "#34495e");
	$("#blocagem2").css("background-color", "#34495e");
	$(".w").css("background-color", "#34495e");
	$(".w").css("opacity", "1");
}
