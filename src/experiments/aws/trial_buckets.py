"""Disposable measurement buckets; permissions and cleanup are recorded per run."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
import re


def bucket_name(trial, run_id):
    method = {'rtmsf': 'rtmsf', 'glue_event_crawler': 'glue', 'athena_msck': 'athena'}[trial['method']]
    files = f'-f{trial["x"]}' if trial['graph'] == 4 else ''
    layout = {'day_first': '-day-tenant', 'tenant_first': '-tenant-day'}.get(trial.get('layout'), '')
    name = f'rtmsf-g{trial["graph"]}-{method}-p{trial["base"]}{files}{layout}-r{trial["repetition"]}-{run_id.replace("_", "-")}'
    if len(name) > 63 or not re.fullmatch(r'[a-z0-9][a-z0-9-]+[a-z0-9]', name):
        raise ValueError('invalid trial bucket name')
    return name


def _unordered(value):
    """CloudTrail selector/field/value lists have no evaluation order."""
    if isinstance(value, dict):
        return {key: _unordered(item) for key, item in value.items()}
    if isinstance(value, list):
        return sorted((_unordered(item) for item in value), key=lambda item: json.dumps(item, sort_keys=True))
    return value


class TrialBuckets:
    def __init__(self, session, outputs, account, region, run_id, evidence_dir):
        from botocore.config import Config
        config = Config(connect_timeout=3, read_timeout=15, retries={'total_max_attempts': 2})
        self.s3, self.sqs, self.trail, self.glue = (session.client(name, region_name=region, config=config)
                                                  for name in ('s3', 'sqs', 'cloudtrail', 'glue'))
        self.outputs, self.account, self.region, self.run_id = outputs, account, region, run_id
        self.path = Path(evidence_dir) / 'trial-buckets.json'
        self.state = json.loads(self.path.read_text()) if self.path.exists() else dict(
            account=account, region=region, run_id=run_id, trials={}, queues={})
        if (self.state['account'], self.state['region'], self.state['run_id']) != (account, region, run_id):
            raise ValueError('bucket manifest belongs to another run or AWS target')
        self.buckets = {key: trial['bucket'] for key, trial in self.state['trials'].items()}

    def _save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.state, indent=2, sort_keys=True) + '\n')

    def provision(self, matrix):
        if self.state['trials']:
            raise ValueError('use a fresh run; an existing bucket manifest is for cleanup only')
        for trial in matrix:
            bucket = bucket_name(trial, self.run_id)
            self.s3.create_bucket(Bucket=bucket, **({'CreateBucketConfiguration': {'LocationConstraint': self.region}}
                                                   if self.region != 'us-east-1' else {}))
            self.state['trials'][trial['id']] = dict(bucket=bucket, method=trial['method'], created=True)
            self.buckets[trial['id']] = bucket
            self._save()
            self.s3.put_public_access_block(Bucket=bucket, PublicAccessBlockConfiguration={
                key: True for key in ('BlockPublicAcls', 'IgnorePublicAcls', 'BlockPublicPolicy', 'RestrictPublicBuckets')})
            role = {'rtmsf': 'TaskRoleArn', 'glue_event_crawler': 'CrawlerRoleArn'}.get(trial['method'])
            if role:
                statements = [dict(Effect='Allow', Principal={'AWS': self.outputs[role]}, Action=actions, Resource=resource)
                              for actions, resource in ((['s3:ListBucket', 's3:GetBucketLocation'], f'arn:aws:s3:::{bucket}'),
                                                        (['s3:GetObject'], f'arn:aws:s3:::{bucket}/fixture/*'))]
                self.s3.put_bucket_policy(Bucket=bucket, Policy=json.dumps(dict(Version='2012-10-17', Statement=statements)))
            self.s3.put_bucket_metrics_configuration(Bucket=bucket, Id='trial', MetricsConfiguration={'Id': 'trial'})
            self.state['trials'][trial['id']]['metrics_enabled_at'] = datetime.now(timezone.utc).isoformat()
            self._save()
        groups = [('rtmsf', [t for t in self.state['trials'].values() if t['method'] == 'rtmsf'], self.outputs['EventQueueUrl'])]
        groups += [(key, [trial], None) for key, trial in self.state['trials'].items() if trial['method'] == 'glue_event_crawler']
        for key, trials, queue in groups:
            if not trials:
                continue
            fresh = queue is None
            if fresh:
                queue = self.sqs.create_queue(QueueName=trials[0]['bucket'], Attributes={
                    'VisibilityTimeout': '180', 'SqsManagedSseEnabled': 'true', 'ReceiveMessageWaitTimeSeconds': '20'})['QueueUrl']
                self.state['queues'][key] = dict(url=queue, created=True)
                self._save()
            attributes = self.sqs.get_queue_attributes(QueueUrl=queue, AttributeNames=['QueueArn', 'Policy'])['Attributes']
            original = json.loads(attributes.get('Policy', '{"Version":"2012-10-17","Statement":[]}'))
            installed = deepcopy(original)
            installed['Statement'].append(dict(Sid='RTMSFTrialBuckets', Effect='Allow', Principal={'Service': 's3.amazonaws.com'},
                Action='sqs:SendMessage', Resource=attributes['QueueArn'], Condition={
                    'StringEquals': {'aws:SourceAccount': self.account},
                    'ArnEquals': {'aws:SourceArn': [f'arn:aws:s3:::{trial["bucket"]}' for trial in trials]}}))
            if fresh:
                installed['Statement'].append(dict(Effect='Allow', Principal={'AWS': self.outputs['CrawlerRoleArn']},
                    Action=['sqs:ReceiveMessage', 'sqs:DeleteMessage', 'sqs:ChangeMessageVisibility', 'sqs:GetQueueAttributes',
                            'sqs:SetQueueAttributes', 'sqs:GetQueueUrl', 'sqs:ListQueueTags'], Resource=attributes['QueueArn']))
            self.state['queues'][key] = dict(url=queue, arn=attributes['QueueArn'], original=original, installed=installed, created=fresh)
            for trial in trials:
                trial['queue_key'] = key
            self._save()
            self.sqs.set_queue_attributes(QueueUrl=queue, Attributes={'Policy': json.dumps(installed)})
        original = self.trail.get_event_selectors(TrailName=self.outputs['TrailName'])
        original = {key: original[key] for key in ('AdvancedEventSelectors', 'EventSelectors') if original.get(key)}
        if 'EventSelectors' in original:
            raise ValueError('pilot requires the stack\'s advanced CloudTrail selectors')
        installed = deepcopy(original)
        installed.setdefault('AdvancedEventSelectors', []).append(dict(Name='RTMSFTrialBuckets', FieldSelectors=[
            {'Field': 'eventCategory', 'Equals': ['Data']}, {'Field': 'resources.type', 'Equals': ['AWS::S3::Object']},
            {'Field': 'readOnly', 'Equals': ['true']},
            {'Field': 'resources.ARN', 'StartsWith': [f'arn:aws:s3:::{bucket}/' for bucket in self.buckets.values()]}]))
        self.state['trail'] = dict(original=original, installed=installed)
        self._save()
        self.trail.put_event_selectors(TrailName=self.outputs['TrailName'], **installed)

    def enable_notifications(self, trial_id=None):
        for key, trial in self.state['trials'].items():
            if (trial_id is not None and key != trial_id) or trial['method'] == 'athena_msck':
                continue
            queue = self.state['queues'][trial['queue_key']]
            self.s3.put_bucket_notification_configuration(Bucket=trial['bucket'], NotificationConfiguration={
                'QueueConfigurations': [{'QueueArn': queue['arn'], 'Events': ['s3:ObjectCreated:*', 's3:ObjectRemoved:*'],
                    'Filter': {'Key': {'FilterRules': [{'Name': 'prefix', 'Value': 'fixture/'}]}}}]})

    def queue_url(self, trial_id):
        return self.state['queues'][self.state['trials'][trial_id]['queue_key']]['url']

    def cleanup(self):
        """Call only after workers/crawlers stop and measurement bounds are frozen."""
        errors = []
        for trial in self.state['trials'].values():
            if trial.get('deleted'):
                continue
            try:
                bucket = trial['bucket']
                self.s3.put_bucket_notification_configuration(Bucket=bucket, NotificationConfiguration={})
                self.s3.delete_bucket_metrics_configuration(Bucket=bucket, Id='trial')
                for page in self.s3.get_paginator('list_objects_v2').paginate(Bucket=bucket):
                    objects = [{'Key': obj['Key']} for obj in page.get('Contents', [])]
                    if objects and self.s3.delete_objects(Bucket=bucket, Delete={'Objects': objects}).get('Errors'):
                        raise RuntimeError('bucket object cleanup failed')
                self.s3.delete_bucket(Bucket=bucket)
                trial['deleted'] = True
            except Exception as error:
                if getattr(error, 'response', {}).get('Error', {}).get('Code') == 'NoSuchBucket':
                    trial['deleted'] = True
                else:
                    errors.append(type(error).__name__ + ': ' + str(error))
            self._save()
        for trial in self.state['trials'].values():
            if not trial.get('table') or trial.get('table_deleted'):
                continue
            try:
                self.glue.delete_table(DatabaseName=self.outputs['GlueDatabaseName'], Name=trial['table'])
                trial['table_deleted'] = True
            except Exception as error:
                if getattr(error, 'response', {}).get('Error', {}).get('Code') == 'EntityNotFoundException':
                    trial['table_deleted'] = True
                else:
                    errors.append(str(error))
            self._save()
        for queue in self.state['queues'].values():
            try:
                if queue.get('created'):
                    if not queue.get('deleted'):
                        self.sqs.delete_queue(QueueUrl=queue['url'])
                        queue['deleted'] = True
                        self._save()
                    continue
                current = json.loads(self.sqs.get_queue_attributes(QueueUrl=queue['url'], AttributeNames=['Policy'])['Attributes']['Policy'])
                if current == queue['installed']:
                    self.sqs.set_queue_attributes(QueueUrl=queue['url'], Attributes={'Policy': json.dumps(queue['original'])})
                elif current != queue['original']:
                    raise RuntimeError('queue policy changed externally; refusing to overwrite it')
            except Exception as error:
                if queue.get('created') and getattr(error, 'response', {}).get('Error', {}).get('Code') in ('AWS.SimpleQueueService.NonExistentQueue', 'QueueDoesNotExist'):
                    queue['deleted'] = True
                else:
                    errors.append(str(error))
        if 'trail' in self.state:
            try:
                current = self.trail.get_event_selectors(TrailName=self.outputs['TrailName'])
                current = {key: current[key] for key in ('AdvancedEventSelectors', 'EventSelectors') if current.get(key)}
                if _unordered(current) == _unordered(self.state['trail']['installed']):
                    self.trail.put_event_selectors(TrailName=self.outputs['TrailName'], **self.state['trail']['original'])
                elif _unordered(current) != _unordered(self.state['trail']['original']):
                    raise RuntimeError('trail selectors changed externally; refusing to overwrite them')
            except Exception as error:
                errors.append(str(error))
        self.state['cleanup_errors'] = errors
        self._save()
        if errors:
            raise RuntimeError('; '.join(errors))
