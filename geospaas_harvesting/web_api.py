import rest_framework.serializers
import rest_framework_filters
from django_filters.rest_framework.filters import CharFilter
from django.db import models
from rest_framework import routers
from rest_framework.viewsets import ModelViewSet

import geospaas_harvesting.models


class ProviderSerializer(rest_framework.serializers.HyperlinkedModelSerializer):
    """Serializer for Provider objects"""
    class Meta:
        model = geospaas_harvesting.models.Provider
        fields = ['id', 'url', 'name', 'normalizer_name', 'crawler_name', 'config']
        extra_kwargs = {
            'url': {'view_name': 'base_viewer:api:harvesting:provider-detail'}
        }

class ProviderFilter(rest_framework_filters.FilterSet):
    """Filterset for providers"""
    class Meta:
        model = geospaas_harvesting.models.Provider
        fields = {
            'name': '__all__',
            'normalizer_name': '__all__',
            'crawler_name': '__all__',
            'config': '__all__',
        }
        filter_overrides = {
            models.JSONField: {
                'filter_class': CharFilter
            }
        }

class ProviderViewSet(ModelViewSet):
    """ViewSet for Providers"""
    queryset = geospaas_harvesting.models.Provider.objects.all()
    serializer_class = ProviderSerializer
    filter_class = ProviderFilter


router = routers.DefaultRouter()
router.register(r'providers', ProviderViewSet)

urlpatterns = router.urls
