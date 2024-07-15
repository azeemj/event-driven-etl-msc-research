from django.shortcuts import render
from .models import Tenant
# Create your views here.


def index(request):
    tenant = request.tenant
    return render(request, 'index.html', {'tenant': tenant})