angular
	.module('BrincosAnimaisApp', [ 'snk' ])
	.controller(
			'BrincosAnimaisController',
			['SkApplication', 'i18n', 'ObjectUtils', 'MGEParameters',
				'GridConfig', 'AngularUtil', 'StringUtils', 'ServiceProxy', 'MessageUtils',
				'SanPopup', '$scope',
function(SkApplication, i18n, ObjectUtils,
		MGEParameters, GridConfig, AngularUtil,
		StringUtils, ServiceProxy, MessageUtils,
		SanPopup, $scope) {

				let self = this;
				
				
				let _dynaformBrincosAnimais
				let _dsBrincosAnimais;
					

				self.onDynaformLoaded = onDynaformLoaded;
				self.customTabsLoader = customTabsLoader;
				self.interceptNavigator = interceptNavigator;
				self.interceptPersonalizedFilter = interceptPersonalizedFilter;
				self.interceptFieldMetadata = interceptFieldMetadata;
				ObjectUtils.implements(self, IDynaformInterceptor);
				
				 
				function onDynaformLoaded(dynaform, dataset) {
					
					if (angular.isDefined(dataset) && dataset.getEntityName() == 'BrincosAnimais') {
									
						_dynaformBrincosAnimais = dynaform;
						_dsBrincosAnimais = dataset;
												
						_dynaformBrincosAnimais.getNavigatorAPI()
							.showRemoveButton(false)
							.showCopyButton(false)
							.showEditButton(false);
						
						_dsBrincosAnimais.beforePostAction((dataset) => {
							if(!!_dsBrincosAnimais.getFieldValue('ATIVO') && !!_dsBrincosAnimais.getFieldValue('DTFIN')
									&& _dsBrincosAnimais.getFieldValue('ATIVO') == 'N') {
								let NUBRINCO = _dsBrincosAnimais.getFieldValue('NUBRINCO');
			                    MessageUtils
		                        .simpleConfirm('Brincos Animais',
		                        'Deseja inativar o brinco também?')
		                        .then(function () {
		                        	ServiceProxy.callService('bovinos@BovinosOperacoesSP.desativarBrinco', {
		                        		NUBRINCO: NUBRINCO
		                        	})
		                        	.then((response) => 
		                        	MessageUtils.showInfo('Brincos', '<b>Brinco '+ NUBRINCO + ' desativado com sucesso.</b>'));
		                        });
							}

                            return true;
                        });
					    						 
					}
				}
				
				function finalizarUso(){
					//alert("Aguardando");
				}
				

				function customTabsLoader(entityName) {
					var customTabs = [];
					return customTabs;
				}

				function interceptNavigator(navigator, dynaform) {
				}

				function interceptPersonalizedFilter(
						personalizedFilter, dataset) {
				}

				function interceptFieldMetadata(fieldMetadata,
						dataset, dynaform) {
				}
			
				
				
				
				
} ]);