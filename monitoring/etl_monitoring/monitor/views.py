from django.shortcuts import render
from .models import Tenant
from .middleware import TenantMiddleware
# Create your views here.


def index(request):
    tenant = request.tenant

    hostname = request.get_host().split(":")[0].lower()
    subdomain = hostname.split('.')[0]

    # Get Grafana dashboard URL based on subdomain
    grafana_dashboard_url = TenantMiddleware.get_grafana_dashboard_url(subdomain)

    context = {
        'grafana_dashboard_url': grafana_dashboard_url,'tenant': tenant
    }
    return render(request, 'index.html', context)