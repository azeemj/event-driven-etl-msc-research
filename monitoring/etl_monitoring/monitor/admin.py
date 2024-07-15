from django.contrib import admin

# Register your models here.
from django.contrib import admin
from .models import Tenant

admin.site.register(Tenant)
