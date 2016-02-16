app.config(function ($translateProvider) {
  $translateProvider.translations('en', {
	TITLE: 'Hello',
	FOO: 'This is a paragraph.',
	BUTTON_LANG_EN: 'English',
	BUTTON_LANG_PT: 'Portuguese',
	INIT_HELLO: 'Hello, visitor! ',
	PAR_1_1: "We've been watching how people all over the world interact with Wikipedia since ",
	PAR_1_2: "and we've got some cool statistics to show you...",
	PAR_2_1: "So far, we've seen a total of ",
	PAR_2_2: "edits (and this number is getting bigger and bigger as you read this text! Only in the last 30 seconds, for example, ",
	PAR_2_3: "new edits were performed - ",
	PAR_2_4: "of them marked as minor). An edit means someone changed some page. Some of these edits are minor edits, used for control and admin tasks only. In our data, there are ",
	PAR_2_5: "major edits, accounting for ",
	PAR_2_6: "of all edits. ",
	PAR_3_1: "All those edits were performed by ",
	PAR_3_2: "different editors/users (excluding bots) on a total of ",
	PAR_3_3: "different pages! This gives us a mean of ",
	PAR_3_4: "edits per user.",
	PAR_4_1: "How big are the edits? ",
	PAR_4_2: "Wikipedia edits have ",
	PAR_4_3: "characters in average. That's a little bit bigger than two tweets!",
	PAR_5_1: "What about different languages?",
	PAR_5_2: "There are reportedly ",
	PAR_5_3: "active different languages Wikipedias. Despite that, we've seen edits in ",
	PAR_5_4: "different languages since the start of our inspection, accounting for ",
	PAR_5_5: "of the active languages.",
	PAR_6_1: "Now, let's see some cool pictures?",
	PAR_6_2: "Most Frequent Languages",
	PAR_6_3: "Most Edited Pages",
	PAR_6_4: "Most Edited Content Pages",
	PAR_6_5: "Most Active Editors",
	PAR_7_1: "All the information you've just read is derived from a ",
	PAR_7_2: "dataset of Wikipedia edits by using a total of ",
	PAR_7_3: "cores to execute an Apache Spark script which took an overall time of ",
	PAR_7_4: "to finish its last batch execution. Check this page in about 30 minutes to see updated statistics.",
	FOOTER_P_1: "Site created by ",
	FOOTER_P_2: " this team ",
	FOOTER_P_3: "as part of GABDI Project at ",
	FOOTER_P_4: "Analytics Lab",
	ABOUT_BTN: "About",
	CONTACT_BTN: "Contact",
	TEAM_BTN: "Team",
	TOP_BTN: "Top",
	COORDINATOR_TEAM: "Coordinator",
	DEVELOPER_TEAM: "Big Data Developer",
	PAR_PRESENT: "We have done the following presentation to explain the infrastructure and details of the tools that we have used on our project."
  });
  $translateProvider.translations('pt', {
	TITLE: 'Olá',
	FOO: 'Isto é um parágrafo.',
	BUTTON_LANG_EN: 'Inglês',
	BUTTON_LANG_PT: 'Português',
	INIT_HELLO: 'Olá, Visitante! ',
	PAR_1_1: "Nós temos observado como as pessoas ao redor de todo o mundo interagem com a Wikipedia desde ",
	PAR_1_2: "e obtivemos algumas estatísticas legais para mostrar... ",
	PAR_2_1: "Até agora, nós vimos um total de ",
	PAR_2_2: "edições (e este número está ficando cada vez maior no momento em que você ler esse texto! Somente nos últimos 30 segundos, por exemplo, ",
	PAR_2_3: "novas edições foram realizadas - ",
	PAR_2_4: "delas são marcadas como \"edições menores\"). Uma edição significa que alguém mudou alguma página. Algumas dessas edições são \"edições menores\", usadas somente para controle e tarefas administrativas. Em nossos dados, existem ",
	PAR_2_5: "\"edições maiores\", totalizando ",
	PAR_2_6: "de todas as edições. ",
	PAR_3_1: "Todas essas edições foram realizadas por ",
	PAR_3_2: "diferentes editores/usuários (excluindo bots) com um total de ",
	PAR_3_3: "diferentes páginas! Isto nos dá uma média de ",
	PAR_3_4: "edições por usuário.",
	PAR_4_1: "Qual o tamanho dessas edições? ",
	PAR_4_2: "Edições da Wikipedia têm ",
	PAR_4_3: "caracteres em média. Isso é um pouco maior que dois tweets!",
	PAR_5_1: "E quanto as diferentes idiomas? ",
	PAR_5_2: "Existem reportadamente ",
	PAR_5_3: "diferentes idiomas ativos na Wikipedia. Apesar disso, nós detectamos edições em ",
	PAR_5_4: "diferentes idiomas desde o começo de nosso experimento, contabilizando ",
	PAR_5_5: "do total de idiomas ativos.",
	PAR_6_1: "Agora, vamos ver algumas figuras legais?",
	PAR_6_2: "Idiomas mais Frequentes",
	PAR_6_3: "Páginas mais Editadas",
	PAR_6_4: "Páginas de Conteúdo mais Editadas",
	PAR_6_5: "Editores mais Ativos",
	PAR_7_1: "Toda informação que você acabou de ler é derivada ",
	PAR_7_2: "dataset de eduções da Wikipedia usando um total de ",
	PAR_7_3: "cores para executar um script Spark que levou em torno de ",
	PAR_7_4: "para finalizar a última execução em batch. Verifique a página novamente em torno de 30 minutos para ver estatísticas atualizadas.",
	FOOTER_P_1: "Site criado por ",
	FOOTER_P_2: "este time ",
	FOOTER_P_3: "como parte do Projeto GABDI do ",
	FOOTER_P_4: "Laboratório Analytics",
	ABOUT_BTN: "Sobre",
	CONTACT_BTN: "Contato",
	TEAM_BTN: "Equipe",
	TOP_BTN: "Topo",
	COORDINATOR_TEAM: "Coordenador",
	DEVELOPER_TEAM: "Desenvolvedor Big Data",
	PAR_PRESENT: "Nós fizemos a seguinte apresentação para explicar a infraestrutura e as ferramentas que utilizamos em nosso projeto."
  });
  $translateProvider.preferredLanguage('pt');
});

app.controller('Ctrl', function ($scope, $translate, tmhDynamicLocale) {
  $scope.changeLanguage = function (key) {
	$translate.use(key);
	tmhDynamicLocale.set(key);
  };
});

app.config(function(tmhDynamicLocaleProvider) {
    tmhDynamicLocaleProvider.localeLocationPattern('bower_components/angular-i18n/angular-locale_{{ locale }}.js');
});

