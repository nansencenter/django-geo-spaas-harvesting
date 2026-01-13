from django.urls import path, include
from rest_framework import routers

import geospaas_harvesting.web_api as web_api
from geospaas_harvesting.web_ui import HarvestingView

router = routers.DefaultRouter()
router.register(r'providers', web_api.ProviderViewSet)

app_name = 'harvesting'
urlpatterns = [
    path('', HarvestingView.as_view(), name='geospaas_harvesting'),
    path('api/', include((router.urls, 'api'))),
]
