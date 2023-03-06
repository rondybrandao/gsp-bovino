angular
	.module('ApontamentoProducaoLeiteApp', [ 'snk' ])
	.controller(
			'ApontamentoProducaoLeiteController',
			['SkApplication', 'i18n', 'ObjectUtils', 'MGEParameters',
				'GridConfig', 'AngularUtil', 'StringUtils', 'ServiceProxy', 'MessageUtils',
				'SanPopup', '$scope',
function(SkApplication, i18n, ObjectUtils,
		MGEParameters, GridConfig, AngularUtil,
		StringUtils, ServiceProxy, MessageUtils,
		SanPopup, $scope) {

				let self = this;
				

				self.onDynaformLoaded = onDynaformLoaded;
				self.customTabsLoader = customTabsLoader;
				self.interceptNavigator = interceptNavigator;
				self.interceptPersonalizedFilter = interceptPersonalizedFilter;
				self.interceptFieldMetadata = interceptFieldMetadata;
				ObjectUtils.implements(self, IDynaformInterceptor);
				
				 
				function onDynaformLoaded(dynaform, dataset) {
					
					if (dataset.getEntityName() == 'ApontamentoProducaoLeite'){
						// _dynaformGerenciaFretes = dynaform;
						// _dsGerenciaFretes = dataset;
					}
					
					
						
					// _dynaformGerenciaFretesDt.getNavigatorAPI()
					// .showAddButton(false)
					// .showCopyButton(false);
						 
			
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