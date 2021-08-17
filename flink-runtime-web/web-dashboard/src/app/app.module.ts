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

import { BrowserModule } from '@angular/platform-browser';
import { APP_INITIALIZER, Injector, NgModule } from '@angular/core';
import { Router } from '@angular/router';
import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import { NZ_I18N, en_US } from 'ng-zorro-antd/i18n';
import { NZ_ICONS, NzIconModule } from 'ng-zorro-antd/icon';
import { NZ_CONFIG, NzConfig } from 'ng-zorro-antd/core/config';
import { NzLayoutModule } from 'ng-zorro-antd/layout';
import { NzMenuModule } from 'ng-zorro-antd/menu';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzBadgeModule } from 'ng-zorro-antd/badge';
import { NzDrawerModule } from 'ng-zorro-antd/drawer';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { FormsModule } from '@angular/forms';
import { HTTP_INTERCEPTORS, HttpClientModule } from '@angular/common/http';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { registerLocaleData } from '@angular/common';
import en from '@angular/common/locales/en';

import {
  BarsOutline,
  BuildOutline,
  CheckCircleOutline,
  DashboardOutline,
  EllipsisOutline,
  AlignLeftOutline,
  FullscreenExitOutline,
  FolderOutline,
  LoginOutline,
  PlayCircleOutline,
  RightOutline,
  PlusOutline,
  QuestionCircleOutline,
  QuestionCircleFill,
  SaveOutline,
  CaretDownOutline,
  CaretUpOutline,
  ScheduleOutline,
  SettingOutline,
  UploadOutline,
  MenuFoldOutline,
  MenuUnfoldOutline,
  DeploymentUnitOutline,
  VerticalLeftOutline,
  VerticalRightOutline,
  RollbackOutline,
  SyncOutline,
  MinusOutline,
  FileTextOutline,
  FullscreenOutline,
  ArrowsAltOutline,
  ReloadOutline,
  DownloadOutline,
  ShrinkOutline,
  PicCenterOutline
} from '@ant-design/icons-angular/icons';

import { StatusService } from 'services';
import { ConfigurationInterface } from 'interfaces';
import { AppInterceptor } from './app.interceptor';
import { NzNotificationModule } from 'ng-zorro-antd/notification';

registerLocaleData(en);

export function AppInitServiceFactory(
  statusService: StatusService,
  injector: Injector
): () => Promise<ConfigurationInterface> {
  return () => {
    return statusService.boot(injector.get<Router>(Router));
  };
}

const ngZorroConfig: NzConfig = {
  notification: { nzMaxStack: 1 }
};

@NgModule({
  declarations: [AppComponent],
  imports: [
    BrowserModule,
    AppRoutingModule,
    FormsModule,
    HttpClientModule,
    BrowserAnimationsModule,
    NzLayoutModule,
    NzIconModule,
    NzMenuModule,
    NzDividerModule,
    NzBadgeModule,
    NzDrawerModule,
    NzAlertModule,
    NzNotificationModule
  ],
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
      useValue: [
        BarsOutline,
        BuildOutline,
        CheckCircleOutline,
        DashboardOutline,
        RightOutline,
        EllipsisOutline,
        FolderOutline,
        LoginOutline,
        PlayCircleOutline,
        PlusOutline,
        QuestionCircleOutline,
        QuestionCircleFill,
        SaveOutline,
        ScheduleOutline,
        CaretDownOutline,
        CaretUpOutline,
        SettingOutline,
        UploadOutline,
        MenuFoldOutline,
        MenuUnfoldOutline,
        DeploymentUnitOutline,
        VerticalLeftOutline,
        VerticalRightOutline,
        RollbackOutline,
        SyncOutline,
        AlignLeftOutline,
        FullscreenExitOutline,
        ReloadOutline,
        DownloadOutline,
        MinusOutline,
        FileTextOutline,
        FullscreenOutline,
        ArrowsAltOutline,
        ShrinkOutline,
        PicCenterOutline
      ]
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
    }
  ],
  bootstrap: [AppComponent]
})
export class AppModule {}
