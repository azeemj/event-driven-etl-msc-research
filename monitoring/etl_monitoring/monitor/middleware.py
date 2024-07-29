from typing import Any
from .models import Tenant
from django.shortcuts import get_object_or_404
from django.urls import reverse
from django.http import HttpResponseRedirect


class TenantMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request) -> Any:
        hostname = request.get_host().split(":")[0].lower()
        subdomain = hostname.split('.')[0]

        # Check if request is for /admin/ and skip tenant lookup
        if request.path.startswith(reverse('admin:index')):
            request.tenant = None  # Set to None for admin interface
        else:
            # Retrieve Tenant object based on subdomain
            request.tenant = get_object_or_404(Tenant, name=subdomain)

        response = self.get_response(request)
        return response

    def get_grafana_dashboard_url(subdomain):
        # Retrieve Tenant object based on subdomain
        
        tenant = get_object_or_404(Tenant, name=subdomain)

        # Construct Grafana dashboard URL
        # Replace with your Grafana base URL
        grafana_base_url = 'http://54.145.59.155:3000/'
        # Replace with your Grafana dashboard slug
        latest_arn = 'arn:aws:states:us-east-1:905418415996:execution:MyStateMachine-un3o6ponq:cf7f47b9-318b-4084-b19e-7d9270cec06d'
        dashboard_slug = 'd/ads0pvv57fx1cc/monitoring-dashboard-copy?orgId=1&refresh=15m'
        return f"{grafana_base_url}dashboard/db/{dashboard_slug}?var-arn={latest_arn}"

# user name =dellpwd:Azeem123
