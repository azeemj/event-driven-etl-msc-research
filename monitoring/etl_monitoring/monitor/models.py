from django.db import models
from django.contrib.auth.models import User

# Create your models here.
from django.contrib.auth.models import User

class Tenant(models.Model):
    name = models.CharField(max_length=200)

    def __str__(self) -> str:
        return self.name

class Profile(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE)
    tenant = models.ForeignKey(Tenant, on_delete=models.CASCADE)

    def __str__(self):
        return self.user.username
    
    