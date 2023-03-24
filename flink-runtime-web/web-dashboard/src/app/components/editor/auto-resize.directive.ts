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

import { Directive, ElementRef, OnDestroy, OnInit } from '@angular/core';
import { animationFrameScheduler, interval, Observable, Subject } from 'rxjs';
import { debounceTime, distinctUntilChanged, takeUntil } from 'rxjs/operators';

import { NzCodeEditorComponent } from 'ng-zorro-antd/code-editor';

@Directive({
  selector: 'nz-code-editor[flinkAutoResize]',
  standalone: true
})
export class AutoResizeDirective implements OnDestroy, OnInit {
  private destroy$ = new Subject<void>();
  hiddenMinimap = false;

  constructor(private elementRef: ElementRef<HTMLElement>, private nzCodeEditorComponent: NzCodeEditorComponent) {}

  public ngOnInit(): void {
    this.createResizeObserver()
      .pipe(
        distinctUntilChanged((prev, curr) => {
          const { width: prevWidth, height: prevHeight } = prev;
          const { width: currWidth, height: currHeight } = curr;
          return prevWidth === currWidth && prevHeight === currHeight;
        }),
        debounceTime(50, animationFrameScheduler),
        takeUntil(this.destroy$)
      )
      .subscribe(() => {
        this.nzCodeEditorComponent.layout();
      });
  }

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  private createResizeObserver(): Observable<DOMRectReadOnly> {
    const stream = new Subject<DOMRectReadOnly>();
    const ResizeObserver = window.ResizeObserver;
    if (ResizeObserver) {
      // https://developer.mozilla.org/en-US/docs/Web/API/ResizeObserver
      // ResizeObserver is supported except IE
      const resizeObserver = new ResizeObserver(entries => {
        for (const entry of entries) {
          if (entry.target === this.elementRef.nativeElement) {
            stream.next(entry.contentRect);
          }
        }
      });
      resizeObserver.observe(this.elementRef.nativeElement);
      this.destroy$.subscribe(() => resizeObserver.disconnect());
    } else if (typeof this.elementRef.nativeElement?.getBoundingClientRect === 'function') {
      interval(500)
        .pipe(takeUntil(this.destroy$))
        .subscribe(() => {
          stream.next(this.elementRef.nativeElement.getBoundingClientRect());
        });
    }
    return stream.asObservable();
  }
}
