# myapp/management/commands/create_initial_data.py
from django.core.management.base import BaseCommand
from django.contrib.auth.models import User
from monitor.models import Tenant, Profile

class Command(BaseCommand):
    help = 'Create initial tenants and users'

    def handle(self, *args, **kwargs):
        tenant1 = Tenant.objects.create(name='tenant1')
        tenant2 = Tenant.objects.create(name='tenant2')

        user1 = User.objects.create_user(username='user1', password='pass')
        user2 = User.objects.create_user(username='user2', password='pass')

        Profile.objects.create(user=user1, tenant=tenant1)
        Profile.objects.create(user=user2, tenant=tenant2)

        self.stdout.write(self.style.SUCCESS('Successfully created initial tenants and users'))
