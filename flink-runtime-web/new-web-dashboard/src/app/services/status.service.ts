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

import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { NavigationEnd, Router } from '@angular/router';
import { EMPTY, fromEvent, interval, merge, Subject } from 'rxjs';
import { debounceTime, filter, map, mapTo, startWith, switchMap, tap } from 'rxjs/operators';
import { BASE_URL } from '../app.config';
import { ConfigurationInterface } from 'interfaces';

@Injectable({
  providedIn: 'root'
})
export class StatusService {
  // collapsed flag
  isCollapsed = false;
  // flink configuration
  configuration: ConfigurationInterface;
  // refresh stream
  refresh$ = new Subject<boolean>().asObservable();
  // manual refresh stream
  private manual$ = new Subject<boolean>();
  // only refresh when tab visibility
  private visible$ = fromEvent(window, 'visibilitychange').pipe(map(e => !(e.target as Document).hidden));

  /** manual refresh **/
  refresh() {
    this.manual$.next(true);
  }

  /** init flink config before booting **/
  boot(router: Router): Promise<ConfigurationInterface> {
    return this.httpClient.get<ConfigurationInterface>(`${BASE_URL}/config`).pipe(tap((data) => {
      this.configuration = data;
      /** navigation end stream **/
      const navigationEnd$ = router.events.pipe(filter(item => (item instanceof NavigationEnd)), mapTo(true));
      const interval$ = interval(this.configuration[ 'refresh-interval' ]).pipe(mapTo(true), startWith(true));
      /** merge tab visible stream, manual refresh stream & navigation end stream **/
      this.refresh$ = merge(this.visible$, this.manual$, navigationEnd$).pipe(
        startWith(true),
        debounceTime(300),
        switchMap(active => active ? interval$ : EMPTY)
      );
    })).toPromise();
  }

  constructor(private httpClient: HttpClient) {
  }
}
