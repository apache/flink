/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { registerLocaleData } from '@angular/common';
import { HTTP_INTERCEPTORS, HttpClientModule } from '@angular/common/http';
import en from '@angular/common/locales/en';
import { APP_INITIALIZER, enableProdMode, importProvidersFrom, Injector } from '@angular/core';
import { bootstrapApplication } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { Router, RouterModule } from '@angular/router';

import { APP_ICONS } from '@flink-runtime-web/app-icons';
import { AppComponent } from '@flink-runtime-web/app.component';
import { AppInterceptor } from '@flink-runtime-web/app.interceptor';
import { Configuration } from '@flink-runtime-web/interfaces';
import { APP_ROUTES } from '@flink-runtime-web/routes';
import { StatusService } from '@flink-runtime-web/services';
import { NZ_CONFIG, NzConfig } from 'ng-zorro-antd/core/config';
import { en_US, NZ_I18N } from 'ng-zorro-antd/i18n';
import { NZ_ICONS } from 'ng-zorro-antd/icon';
import { NzNotificationModule } from 'ng-zorro-antd/notification';

import { environment } from './environments/environment';

if (environment.production) {
  enableProdMode();
}

registerLocaleData(en);

export function AppInitServiceFactory(
  statusService: StatusService,
  injector: Injector
): () => Promise<Configuration | undefined> {
  return () => {
    return statusService.boot(injector.get<Router>(Router));
  };
}

const ngZorroConfig: NzConfig = {
  notification: { nzMaxStack: 1 }
};

bootstrapApplication(AppComponent, {
  providers: [
    {
      provide: NZ_I18N,
      useValue: en_US
    },
    {
      provide: NZ_CONFIG,
      useValue: ngZorroConfig
    },
    {
      provide: NZ_ICONS,
      useValue: APP_ICONS
    },
    {
      provide: HTTP_INTERCEPTORS,
      useClass: AppInterceptor,
      multi: true
    },
    {
      provide: APP_INITIALIZER,
      useFactory: AppInitServiceFactory,
      deps: [StatusService, Injector],
      multi: true
    },
    importProvidersFrom(HttpClientModule),
    importProvidersFrom(BrowserAnimationsModule),
    importProvidersFrom(NzNotificationModule),
    importProvidersFrom(
      RouterModule.forRoot([...APP_ROUTES], {
        useHash: true
      })
    )
  ]
}).catch(err => console.error(err));
