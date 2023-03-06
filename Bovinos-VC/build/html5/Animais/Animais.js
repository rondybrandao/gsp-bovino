angular
	.module('AnimaisApp', [ 'snk' ])
	.controller(
			'AnimaisController',
			['SkApplication', 'i18n', 'ObjectUtils', 'MGEParameters',
				'GridConfig', 'AngularUtil', 'StringUtils', 'ServiceProxy', 'MessageUtils',
				'SanPopup', '$scope',
function(SkApplication, i18n, ObjectUtils,
		MGEParameters, GridConfig, AngularUtil,
		StringUtils, ServiceProxy, MessageUtils,
		SanPopup, $scope) {

				let self = this;
				
				ObjectUtils.implements(self, IDataSetObserver);
				ObjectUtils.implements(self, IDynaformInterceptor);
				ObjectUtils.implements(self, IDatagridInterceptor);
				ObjectUtils.implements(self, IFormInterceptor);
				

				self.onDynaformLoaded = onDynaformLoaded;
				self.customTabsLoader = customTabsLoader;
				self.interceptNavigator = interceptNavigator;
				self.interceptPersonalizedFilter = interceptPersonalizedFilter;
				self.interceptFieldMetadata = interceptFieldMetadata;
				self.interceptBuildField = interceptBuildField;
				
				 
				function onDynaformLoaded(dynaform, dataset) {
					
					if (dataset.getEntityName() == 'Animais'){
					}
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
			
				function interceptBuildField(fieldName, dataset, fieldProp, scope) {

				}
				
				
				
} ]);