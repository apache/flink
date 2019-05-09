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
import { debounceTime, filter, map, mapTo, share, startWith, switchMap, tap } from 'rxjs/operators';
import { BASE_URL } from '../app.config';
import { ConfigurationInterface } from 'interfaces';

@Injectable({
  providedIn: 'root'
})
export class StatusService {
  /**
   * Error server response message cache list
   */
  listOfErrorMessage: string[] = [];
  /**
   * Flink configuration from backend
   */
  configuration: ConfigurationInterface;
  /**
   * Refresh stream generated from the configuration
   */
  refresh$ = new Subject<boolean>().asObservable();
  /**
   * Force refresh stream trigger manually
   */
  private forceRefresh$ = new Subject<boolean>();
  /**
   * Document visibility stream
   */
  private visibility$ = fromEvent(window, 'visibilitychange').pipe(map(e => !(e.target as Document).hidden));

  /**
   * Trigger force refresh
   */
  forceRefresh() {
    this.forceRefresh$.next(true);
  }

  /**
   * Create refresh stream when APP_INITIALIZER
   * refresh interval stream will be regenerated when NavigationEnd || forceRefresh || visibility change
   * @param router
   */
  boot(router: Router): Promise<ConfigurationInterface> {
    return this.httpClient
      .get<ConfigurationInterface>(`${BASE_URL}/config`)
      .pipe(
        tap(data => {
          this.configuration = data;
          const navigationEnd$ = router.events.pipe(
            filter(item => item instanceof NavigationEnd),
            mapTo(true)
          );
          const interval$ = interval(this.configuration['refresh-interval']).pipe(
            mapTo(true),
            startWith(true)
          );
          this.refresh$ = merge(this.visibility$, this.forceRefresh$, navigationEnd$).pipe(
            startWith(true),
            debounceTime(300),
            switchMap(active => (active ? interval$ : EMPTY)),
            share()
          );
        })
      )
      .toPromise();
  }

  constructor(private httpClient: HttpClient) {}
}
