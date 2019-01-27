/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *       http://www.apache.org/licenses/LICENSE-2.0
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

import { BrowserModule } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { registerLocaleData } from '@angular/common';
import { HTTP_INTERCEPTORS, HttpClientModule } from '@angular/common/http';
import { APP_INITIALIZER, Injector, NgModule } from '@angular/core';
import en from '@angular/common/locales/en';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';

import { NZ_MONACO_EDITOR_CONFIG, NzMonacoEditorConfig, NzMonacoEditorTheme } from '@ng-zorro/ng-plus';
import { NgZorroAntdModule, NZ_I18N, en_US, NZ_ICONS } from 'ng-zorro-antd';
import { StatusService, ConfigService } from 'flink-services';
import { ShareModule } from 'flink-share/share.module';
import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import { AppInterceptor } from './app.interceptor';

registerLocaleData(en);

export function NzMonacoEditorConfigFactory(configService: ConfigService): NzMonacoEditorConfig {
  return {
    defaultEditorOptions: { language: 'apex' },
    defaultTheme        : `${configService.theme}` as NzMonacoEditorTheme
  };
}

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
  InterationTwoTone,
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
  ShrinkOutline,
  PicCenterOutline
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
    {
      provide   : NZ_MONACO_EDITOR_CONFIG,
      useFactory: NzMonacoEditorConfigFactory,
      deps      : [ ConfigService ],
      multi     : true
    },
    {
      provide : HTTP_INTERCEPTORS,
      useClass: AppInterceptor,
      multi   : true
    },
    {
      provide : NZ_I18N,
      useValue: en_US
    },
    {
      provide: NZ_ICONS, useValue: [
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
        InterationTwoTone,
        DeploymentUnitOutline,
        VerticalLeftOutline,
        VerticalRightOutline,
        RollbackOutline,
        SyncOutline,
        AlignLeftOutline,
        FullscreenExitOutline,
        MinusOutline,
        FileTextOutline,
        FullscreenOutline,
        ArrowsAltOutline,
        ShrinkOutline,
        PicCenterOutline
      ]
    },
    {
      provide   : APP_INITIALIZER,
      useFactory: AppInitServiceFactory,
      deps      : [ StatusService, Injector ],
      multi     : true
    }
  ],
  bootstrap   : [ AppComponent ]
})
export class AppModule {
}
