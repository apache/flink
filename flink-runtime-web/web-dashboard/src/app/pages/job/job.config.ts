import { InjectionToken } from '@angular/core';

import { ModuleConfig } from '@flink-runtime-web/core/module-config';

export type JobModuleConfig = Pick<ModuleConfig, 'routerTabs'>;

export const JOB_MODULE_DEFAULT_CONFIG: Required<JobModuleConfig> = {
  routerTabs: [
    { title: 'Overview', path: 'overview' },
    { title: 'Exceptions', path: 'exceptions' },
    { title: 'TimeLine', path: 'timeline' },
    { title: 'Checkpoints', path: 'checkpoints' },
    { title: 'Configuration', path: 'configuration' }
  ]
};

export const JOB_MODULE_CONFIG = new InjectionToken<JobModuleConfig>('job-module-config', {
  providedIn: 'root',
  factory: () => JOB_MODULE_DEFAULT_CONFIG
});
