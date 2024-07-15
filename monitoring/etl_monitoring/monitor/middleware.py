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
    
#user name =dellpwd:Azeem123