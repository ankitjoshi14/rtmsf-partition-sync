from copy import deepcopy
import json
from types import SimpleNamespace
import pytest


def test_trial_buckets_are_named_isolated_and_restore_permissions(tmp_path):
    from experiments.aws.trial_buckets import TrialBuckets

    original_policy = {'Version': '2012-10-17', 'Statement': [{'Sid': 'existing'}]}
    selectors = [{'Name': 'original', 'FieldSelectors': []}]
    calls, policies = [], {'worker-queue': deepcopy(original_policy), 'crawler-queue': deepcopy(original_policy)}
    class Client:
        def __getattr__(self, name):
            def call(**kwargs):
                calls.append((name, kwargs))
                if name == 'create_queue':
                    queue = kwargs['QueueName']
                    policies[queue] = {'Version': '2012-10-17', 'Statement': []}
                    return {'QueueUrl': queue}
                if name == 'delete_queue':
                    policies.pop(kwargs['QueueUrl'], None)
                if name == 'get_queue_attributes':
                    return {'Attributes': {'QueueArn': 'arn:' + kwargs['QueueUrl'], 'Policy': json.dumps(policies[kwargs['QueueUrl']])}}
                if name == 'set_queue_attributes':
                    policies[kwargs['QueueUrl']] = json.loads(kwargs['Attributes']['Policy'])
                if name == 'get_event_selectors':
                    result = deepcopy(selectors)
                    for selector in result:
                        selector['FieldSelectors'].reverse()  # AWS reorders these on read.
                    return {'AdvancedEventSelectors': result}
                if name == 'put_event_selectors':
                    selectors[:] = deepcopy(kwargs['AdvancedEventSelectors'])
                if name == 'get_paginator':
                    return SimpleNamespace(paginate=lambda **kw: [{'Contents': [{'Key': 'fixture/test'}]}])
                return {}
            return call
        def get_paginator(self, name):
            return SimpleNamespace(paginate=lambda **kw: [{'Contents': [{'Key': 'fixture/test'}]}])

    outputs = dict(EventQueueUrl='worker-queue', CrawlerEventQueueUrl='crawler-queue',
                   TaskRoleArn='arn:worker', CrawlerRoleArn='arn:crawler', TrailName='trail')
    client = Client()
    helper = TrialBuckets(SimpleNamespace(client=lambda *a, **k: client), outputs,
                          '123456789012', 'us-west-2', 'p20260922_120000_abcd', tmp_path)
    matrix = [dict(id='first', graph=2, method='rtmsf', base=1000, x=1000, repetition=1),
              dict(id='second', graph=4, method='glue_event_crawler', base=1000, x=100, repetition=1)]
    helper.provision(matrix)
    assert helper.buckets['first'].startswith('rtmsf-g2-rtmsf-p1000-r1-')
    assert helper.buckets['second'].startswith('rtmsf-g4-glue-p1000-f100-r1-')
    assert len(set(helper.buckets.values())) == 2
    assert helper.queue_url('second') != outputs['CrawlerEventQueueUrl']
    assert all(len(name) <= 63 for name in helper.buckets.values())
    assert policies['worker-queue']['Statement'][0] == original_policy['Statement'][0]
    assert policies['crawler-queue'] == original_policy
    helper.enable_notifications()
    grants = [params for action, params in calls if action == 'put_bucket_policy']
    assert {json.loads(g['Policy'])['Statement'][0]['Principal']['AWS'] for g in grants} == {'arn:worker', 'arn:crawler'}
    assert len([c for c in calls if c[0] == 'put_bucket_metrics_configuration']) == 2
    helper.cleanup()
    assert policies == {'worker-queue': original_policy, 'crawler-queue': original_policy}
    assert selectors == [{'Name': 'original', 'FieldSelectors': []}]
    assert {p['Bucket'] for a, p in calls if a == 'delete_bucket'} == set(helper.buckets.values())
    state = json.loads((tmp_path / 'trial-buckets.json').read_text())
    assert all(t['deleted'] for t in state['trials'].values())


def test_trial_bucket_names_do_not_collide_across_runs():
    from experiments.aws.trial_buckets import bucket_name
    trial = dict(graph=2, method='athena_msck', base=1000, x=1000, repetition=1)
    assert bucket_name(trial, 'p20260922_120000_abcd') != bucket_name(trial, 'p20260922_120000_efgh')


def test_cleanup_preserves_a_real_external_selector_change(tmp_path):
    from experiments.aws.trial_buckets import TrialBuckets
    helper = TrialBuckets.__new__(TrialBuckets)
    helper.path = tmp_path / 'state.json'
    helper.outputs = {'TrailName': 'trail'}
    helper.state = dict(trials={}, queues={}, trail={'installed': {'AdvancedEventSelectors': [{'Name': 'ours'}]},
                                                     'original': {'AdvancedEventSelectors': [{'Name': 'original'}]}})
    helper.trail = SimpleNamespace(get_event_selectors=lambda **kw: {'AdvancedEventSelectors': [{'Name': 'external'}]},
                                   put_event_selectors=lambda **kw: pytest.fail('must not overwrite external changes'))
    with pytest.raises(RuntimeError, match='changed externally'):
        helper.cleanup()
