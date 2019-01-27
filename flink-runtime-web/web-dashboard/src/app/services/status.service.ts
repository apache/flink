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

import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { NavigationEnd, Router } from '@angular/router';
import { ConfigService } from './config.service';
import { EMPTY, fromEvent, interval, merge, Subject } from 'rxjs';
import { debounceTime, filter, map, mapTo, startWith, switchMap, tap } from 'rxjs/operators';
import { ConfigurationInterface } from 'flink-interfaces';

@Injectable({
  providedIn: 'root'
})
export class StatusService {
  infoList = [];
  // collapsed flag
  isCollapsed = false;
  // loading flag
  isLoading = false;
  // flink configuration
  configuration: ConfigurationInterface;
  // refresh stream
  refresh$ = new Subject<boolean>().asObservable();
  // manual refresh stream
  private manual$ = new Subject<boolean>();
  // only refresh when visibility
  private focus$ = fromEvent(window, 'visibilitychange').pipe(map(e => !(e.target as Document).hidden));

  manualRefresh() {
    this.manual$.next(true);
  }

  /** init flink config before booting **/
  boot(router: Router): Promise<ConfigurationInterface> {
    this.isLoading = true;
    return this.httpClient.get<ConfigurationInterface>(`${this.configService.BASE_URL}/config`).pipe(tap((data) => {
      this.configuration = data;
      const navigationEnd$ = router.events.pipe(filter(item => (item instanceof NavigationEnd)), mapTo(true));
      const interval$ = interval(this.configuration[ 'refresh-interval' ]).pipe(mapTo(true), startWith(true));
      this.refresh$ = merge(this.focus$, this.manual$, navigationEnd$).pipe(
        startWith(true),
        debounceTime(300),
        switchMap(active => active ? interval$ : EMPTY)
      );
      this.isLoading = false;
    })).toPromise();
  }

  constructor(private httpClient: HttpClient, private configService: ConfigService) {
  }
}
