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
import { EMPTY, fromEvent, interval, merge, Observable, Subject } from 'rxjs';
import { debounceTime, filter, map, mapTo, share, startWith, switchMap, tap } from 'rxjs/operators';

import { Configuration } from '@flink-runtime-web/interfaces';

import { ConfigService } from './config.service';

@Injectable({
  providedIn: 'root'
})
export class StatusService {
  constructor(private readonly httpClient: HttpClient, private readonly configService: ConfigService) {}

  /** Error server response message cache list. */
  public listOfErrorMessage: string[] = [];

  /** Flink configuration from backend. */
  public configuration: Configuration;

  public refresh$: Observable<boolean>;
  private readonly forceRefresh$ = new Subject<boolean>();
  private readonly visibility$ = fromEvent(window, 'visibilitychange').pipe(map(e => !(e.target as Document).hidden));

  public forceRefresh(): void {
    this.forceRefresh$.next(true);
  }

  /**
   * Create refresh stream when APP_INITIALIZER
   * refresh interval stream will be regenerated when NavigationEnd || forceRefresh || visibility change
   *
   * @param router
   */
  public boot(router: Router): Promise<Configuration | undefined> {
    return this.httpClient
      .get<Configuration>(`${this.configService.BASE_URL}/config`)
      .pipe(
        tap(data => {
          this.configuration = data;
          const navigationEnd$ = router.events.pipe(
            filter(item => item instanceof NavigationEnd),
            mapTo(true)
          );
          const interval$ = interval(this.configuration['refresh-interval']).pipe(mapTo(true), startWith(true));
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
}
