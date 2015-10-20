var position;
var rep;
var seta;
var textoBlocagens;

directory.ShellView = Backbone.View.extend({

	initialize : function() {
		position = 1;
		rep = false;
		seta = false;
		textoBlocagens = "Execuções curriculares mais comuns dos alunos. Estas são execuções curriculares que muitos alunos cursaram";
	},

	render : function() {
		this.$el.html(this.template());
		return this;
	},

	events : {
		"click #grade1" : "fluxograma",
		"click #grade2" : "blocagemComum",
		"click #bloc1" : "blocagem1",
		"click #bloc2" : "blocagem2",
		"click #bloc3" : "blocagem3",
		"click #bloc4" : "blocagem4",
		"click #correlacao" : "correlacao",
		"click #taxareprovacao" : "taxareprovacao",
		"click #checkSetas" : "checkSetas",

	},

	fluxograma : function() {
		if (position == 1)
			console.log("Já está no fluxograma");
		else {
			directory.coursesView.setPositions2("http://analytics.lsd.ufcg.edu.br/ccc/disciplinasPorPeriodo", 0);
			if (seta) {
				directory.coursesView.connect("http://analytics.lsd.ufcg.edu.br/ccc/preRequisito");
			}

			position = 1;
			console.log("Fluxograma comum");
			$("#idtitulo").text("Plano de curso");
			$("#iddescricao").text("Este é o plano de curso proposto pela coordenação. Contém as disciplinas obrigatórias e suas relações de pré-requisito, onde cada coluna representa um semestre letivo.");

			$("#botao_legenda").hide();
			$("#setas").show();

			$("#taxareprovacao").attr('checked', false);
			rep = false;

			var dialogHTML = $("#dialog_fluxograma").html();
			$("#dialog").html(dialogHTML);

			$("#botao_analise").hide();

		}
	},


	correlacao : function() {
		if (position == 7)
			console.log("Já está na correlação");
		else {
			directory.coursesView.setPositions2("http://analytics.lsd.ufcg.edu.br/ccc/disciplinasPorPeriodo", 0);
			directory.coursesView.correlacao("http://analytics.lsd.ufcg.edu.br/ccc//correlacoes/0.545");

			position = 7;
			console.log("Correlacao");
			$("#idtitulo").text("Correlação entre disciplinas");
			$("#iddescricao").text("As linhas no fluxograma abaixo representam a correlação entre as notas das disciplinas. As correlações encontradas são quase sempre positivas, e significam que se um aluno foi melhor avaliado que seus colegas em uma das duas disciplinas ligadas por uma linha, há uma alta probabilidade de que ele será avaliado da mesma forma também na outra; se ele foi avaliado abaixo da média de seus colegas, ele provavelmente irá obter uma avaliação também abaixo da média na outra. Quanto mais forte a correlação entre as disciplinas, mais espessa é a linha que as liga.");
			$("#botao_legenda").show();
			var linkText = $("#legenda_correlacao").html();
			$("#legendaParaMostrar").html(linkText);
			$("#setas").hide();

			$("#taxareprovacao").attr('checked', false);
			rep = false;
			
			var dialogHTML = $("#dialog_correlacao").html();
			$("#dialog").html(dialogHTML);

			$("#botao_analise").show();

		}
	},


	blocagemComum : function() {
		if (position == 2)
			console.log("Já está no execução curricular comum");
		else {
			directory.coursesView.setPositions2("http://analytics.lsd.ufcg.edu.br/ccc/maioresFrequencias", 1);

			position = 2;
			console.log("Blocagem comum");
			$("#idtitulo").text("Execução curricular mais comum");
			$("#iddescricao").html("Esta é a execução curricular onde as disciplinas são mostradas nos semestres os quais elas tem sido cursadas com <b>maior frequência pelos alunos</b>. Ao passar o mouse em cada disciplina, aparecem o segundo e terceiro semestres nos quais ela é cursada com mais frequência.");
			$("#botao_legenda").show();

			var linkText = $("#legenda_blocagem").html();
			$("#legendaParaMostrar").html(linkText);
			$("#setas").hide();

			$("#taxareprovacao").attr('checked', false);
			rep = false;

			var dialogHTML = $("#dialog_execucao_comum").html();
			$("#dialog").html(dialogHTML);

			$("#botao_analise").show();
		}
	},

	blocagem1 : function() {

		if (position == 3)
			console.log("Já está no execução curricular 1");
		else {
			//directory.coursesView.setPositions("data/cls_fa/1_cluster.json", 0);
			directory.coursesView.setPositions2("http://analytics.lsd.ufcg.edu.br/ccc/clusters/1", 0);

			position = 3;
			console.log("Blocagem 1");
			$("#idtitulo").text("Execuções > Execução curricular 1");
			$("#iddescricao").text(textoBlocagens);
			$("#iddescricao").append("<br/> <br/>Os alunos neste grupo tendem, a partir do segundo período, a cursar cinco disciplinas obrigatórias por período. No sétimo período esse valor cai para quatro disciplinas. Isso caracteriza um aluno que tem uma tendência de manter um número fixo de disciplinas obrigatórias. Podemos perceber nesse grupo que esses alunos possuem um planejamento maior para balancear a sobre-carga das obrigatórias.")
			$("#botao_legenda").hide()
			$("#setas").hide();

			$("#taxareprovacao").attr('checked', false);
			rep = false;
			
			var dialogHTML = $("#dialog4").html();
			$("#dialog").html(dialogHTML);

			$("#botao_analise").show();
		}
	},

	blocagem2 : function() {
		if (position == 4)
			console.log("Já está no execução curricular 2");
		else {
			//directory.coursesView.setPositions("data/cls_fa/2_cluster.json", 0);
			directory.coursesView.setPositions2("http://analytics.lsd.ufcg.edu.br/ccc/clusters/2", 0);
			position = 4;
			console.log("Blocagem 2");
			$("#idtitulo").text("Execuções > Execução curricular 2");
			$("#iddescricao").text(textoBlocagens);

			$("#iddescricao").append("<br/> <br/> É o grupo de alunos que segue mais fielmente o plano de curso oficial, e que ao longo do tempo vai colocando menos disciplinas obrigatórias por semestre. As disciplinas do quinto período, tradicionalmente são tidas como difíceis pelos alunos, e são separadas e redistribuídas nesta execução curricular. Essa distribuição leva ao aumento em um período na duração do curso.")
			$("#botao_legenda").hide()
			$("#setas").hide();

			$("#taxareprovacao").attr('checked', false);
			rep = false;
			
			var dialogHTML = $("#dialog5").html();
			$("#dialog").html(dialogHTML);

			$("#botao_analise").show();
		}
	},

	blocagem3 : function() {
		if (position == 5)
			console.log("Já está no execução curricular 3");
		else {
			//directory.coursesView.setPositions("data/cls_fa/3_cluster.json", 0);
			directory.coursesView.setPositions2("http://analytics.lsd.ufcg.edu.br/ccc/clusters/3", 0);
			position = 5;
			console.log("Blocagem 3");
			$("#idtitulo").text("Execuções > Execução curricular 3");
			$("#iddescricao").text(textoBlocagens);
			$("#iddescricao").append("<br/> <br/>  Representa uma execução curricular com algumas semelhanças com o plano de curso oficial, porém neste caso acontecem algumas reprovações em disciplinas do começo do curso (com a presença de Cálculo I no segundo período e Cálculo II no terceiro período).")

			$("#botao_legenda").hide()
			$("#setas").hide();

			$("#taxareprovacao").attr('checked', false);
			rep = false;
			
			var dialogHTML = $("#dialog6").html();
			$("#dialog").html(dialogHTML);

			$("#botao_analise").show();
		}
	},

	taxareprovacao : function() {
		if (rep == true) {
			console.log("Já está na taxa de sucesso");
			rep = false;
			var temp = position;
			position = 0;
			if (temp == 1) {
				this.fluxograma();
			}
			if (temp == 2) {
				this.blocagemComum();
			}
			if (temp == 3) {
				this.blocagem1();
			}
			if (temp == 4) {
				this.blocagem2();
			}
			if (temp == 5) {
				this.blocagem3();
			}
			if (temp == 7) {
				this.correlacao();
			}
		} else {
			//directory.coursesView.setPositions("http://analytics.lsd.ufcg.edu.br/ccc/getDisciplinasPorPeriodo", 0);
			directory.coursesView.taxaReprovacao("http://analytics.lsd.ufcg.edu.br/ccc/reprovacoes", 0);

			rep = true;
			console.log("taxareprovacao");
			$("#idtitulo").text("Taxa de sucesso em cada disciplina");
			$("#iddescricao").text("A taxa de sucesso da disciplina representa a porcentagem de aprovados da mesma, nos dados estudados.");
			$("#botao_legenda").show();
			var linkText = $("#legenda_reprovacao").html();
			$("#legendaParaMostrar").html(linkText);

			var dialogHTML = $("#dialog_taxa_sucesso").html();
			$("#dialog").html(dialogHTML);

			$("#botao_analise").show();

		}
	},

	// revisar o local desse codigo
	checkSetas : function() {
		if (seta == false) {
			seta = true;
			directory.coursesView.connect("http://analytics.lsd.ufcg.edu.br/ccc/preRequisito", seta);
		} else {
			seta = false;
			jsplumbdeleteEveryEndpoint();
			$(".w").unbind('mouseover mouseout');

		}
	}

}); 
