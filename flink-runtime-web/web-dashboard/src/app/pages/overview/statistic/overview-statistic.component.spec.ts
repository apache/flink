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

import { ComponentFixture, TestBed } from '@angular/core/testing';
import { of } from 'rxjs';

import { OverviewWithApplicationStatistics } from '@flink-runtime-web/interfaces';
import { beforeEach, describe, expect, it } from 'vitest';

import { OverviewStatisticComponent } from './overview-statistic.component';

const mockStatistic: OverviewWithApplicationStatistics = {
  taskmanagers: 5,
  'slots-total': 20,
  'slots-available': 10,
  'applications-running': 3,
  'applications-finished': 7,
  'applications-cancelled': 1,
  'applications-failed': 2,
  'flink-version': '2.4-SNAPSHOT',
  'flink-commit': 'abcdef0'
};

describe('OverviewStatisticComponent', () => {
  let fixture: ComponentFixture<OverviewStatisticComponent>;
  let element: HTMLElement;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [OverviewStatisticComponent]
    }).compileComponents();
    fixture = TestBed.createComponent(OverviewStatisticComponent);
    element = fixture.nativeElement as HTMLElement;
  });

  it('renders both statistic cards populated from the bound observable', () => {
    fixture.componentInstance.statisticData$ = of(mockStatistic);
    fixture.detectChanges();

    const text = element.textContent ?? '';
    expect(text).toContain('Available Task Slots');
    expect(text).toContain('Running Applications');

    // The two ".total" headline numbers are available slots and running applications.
    const totals = Array.from(element.querySelectorAll('.total')).map(el => el.textContent?.trim());
    expect(totals).toEqual(['10', '3']);

    // Footer breakdown values are rendered through the number pipe.
    expect(text).toContain('Task Managers');
    expect(text).toContain('5');
    expect(text).toContain('Finished');
    expect(text).toContain('7');
  });

  it('renders no card content until statistic data arrives', () => {
    // No statisticData$ input, so the outer *ngIf keeps the cards out of the DOM.
    fixture.detectChanges();

    expect(element.querySelector('[nz-row]')).toBeNull();
    expect(element.querySelectorAll('.total').length).toBe(0);
  });
});
