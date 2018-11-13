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

/** angular module **/
import { BrowserModule } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { APP_INITIALIZER, Injector, NgModule } from '@angular/core';
import { Router } from '@angular/router';
import { FormsModule } from '@angular/forms';
import { HTTP_INTERCEPTORS, HttpClientModule } from '@angular/common/http';

/** ng-zorro-antd module **/
import { NgZorroAntdModule, NZ_I18N, en_US, NZ_ICONS } from 'ng-zorro-antd';

/** routing module **/
import { AppRoutingModule } from './app-routing.module';

/** http interceptor **/
import { AppInterceptor } from './app.interceptor';
import { StatusService } from 'services';

/** share module **/
import { ShareModule } from 'share/share.module';

/** root component **/
import { AppComponent } from './app.component';

/** register en language **/
import { registerLocaleData } from '@angular/common';
import en from '@angular/common/locales/en';

registerLocaleData(en);

/** run StatusService.boot when application initialized **/
export function AppInitServiceFactory(statusService: StatusService, injector: Injector): Function {
  return () => {
    return statusService.boot(injector.get(Router));
  };
}


import {
  BarsOutline,
  BuildOutline,
  CheckCircleOutline,
  DashboardOutline,
  EllipsisOutline,
  FolderOutline,
  LoginOutline,
  PlayCircleOutline,
  PlusOutline,
  QuestionCircleOutline,
  SaveOutline,
  CaretDownOutline,
  CaretUpOutline,
  ScheduleOutline,
  InterationTwoTone,
  SettingOutline,
  UploadOutline,
  MenuFoldOutline,
  MenuUnfoldOutline,
  DeploymentUnitOutline,
  SyncOutline
} from '@ant-design/icons-angular/icons';

@NgModule({
  declarations: [
    AppComponent
  ],
  imports     : [
    BrowserModule,
    BrowserAnimationsModule,
    FormsModule,
    HttpClientModule,
    NgZorroAntdModule,
    ShareModule,
    AppRoutingModule
  ],
  providers   : [
    /** http interceptor to handle error http response **/
    {
      provide : HTTP_INTERCEPTORS,
      useClass: AppInterceptor,
      multi   : true
    },
    /** i18n setting **/
    {
      provide : NZ_I18N,
      useValue: en_US
    },
    /** AppInitServiceFactory be executed when application initialized **/
    {
      provide   : APP_INITIALIZER,
      useFactory: AppInitServiceFactory,
      deps      : [ StatusService, Injector ],
      multi     : true
    },
    /** static icons import **/
    {
      provide: NZ_ICONS, useValue: [
        BarsOutline,
        BuildOutline,
        CheckCircleOutline,
        DashboardOutline,
        EllipsisOutline,
        FolderOutline,
        LoginOutline,
        PlayCircleOutline,
        PlusOutline,
        QuestionCircleOutline,
        SaveOutline,
        ScheduleOutline,
        CaretDownOutline,
        CaretUpOutline,
        SettingOutline,
        UploadOutline,
        MenuFoldOutline,
        MenuUnfoldOutline,
        InterationTwoTone,
        DeploymentUnitOutline,
        SyncOutline
      ]
    },
  ],
  bootstrap   : [ AppComponent ]
})
export class AppModule {
}
