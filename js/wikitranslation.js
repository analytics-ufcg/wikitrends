app.config(function ($translateProvider) {
  $translateProvider.translations('en', {
	TITLE: 'Hello',
	FOO: 'This is a paragraph.',
	BUTTON_LANG_EN: 'english',
	BUTTON_LANG_PT: 'portuguese',
	INIT_HELLO: 'Hello, visitor!',
	PAR_1_1: "We've been watching how people all over the world interact with Wikipedia since",
	PAR_1_2: "and we've got some cool statistics to show you..."
  });
  $translateProvider.translations('pt', {
	TITLE: 'Olá',
	FOO: 'Isto é um parágrafo.',
	BUTTON_LANG_EN: 'inglês',
	BUTTON_LANG_PT: 'português',
	INIT_HELLO: 'Olá, Visitante!',
	PAR_1_1: "Nós temos observado como as pessoas ao redor de todo o mundo interagem com a Wikipedia desde",
	PAR_1_2: "e obtivemos algumas estatísticas legais para mostrar..."
  });
  $translateProvider.preferredLanguage('pt');
});

app.controller('Ctrl', function ($scope, $translate) {
  $scope.changeLanguage = function (key) {
	$translate.use(key);
  };
});
