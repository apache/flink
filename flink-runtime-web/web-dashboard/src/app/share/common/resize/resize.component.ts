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

import { DOCUMENT } from '@angular/common';
import {
  ChangeDetectionStrategy,
  Component,
  ElementRef,
  EventEmitter,
  Inject,
  Input,
  OnDestroy,
  OnInit,
  Output,
  ViewChild
} from '@angular/core';
import { fromEvent, Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
import { isNil } from 'utils';

export enum ResizeModeEnums {
  Vertical = 'vertical',
  Horizontal = 'horizontal'
}

export type ResizeMode = ResizeModeEnums | 'vertical' | 'horizontal';

@Component({
  selector: 'flink-resize',
  templateUrl: './resize.component.html',
  styleUrls: ['./resize.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class ResizeComponent implements OnInit, OnDestroy {
  private destroy$ = new Subject();
  private isMoving = false;
  @Input() left = 0;
  @Input() top = 0;
  @Input() baseElement: ElementRef;
  @Input() resizeMin = 0;
  @Input() mode: ResizeMode = ResizeModeEnums.Vertical;
  @Output() leftChange = new EventEmitter();
  @Output() topChange = new EventEmitter();
  @Output() resizeEnd = new EventEmitter<{ left: number; top: number }>();
  @ViewChild('trigger') trigger: ElementRef<HTMLDivElement>;

  startMove() {
    this.isMoving = true;
  }

  /**
   * Get recursive top
   * @param e
   */
  getResizeTop(e: MouseEvent): number {
    const getOffsetTop = (elem: HTMLElement | null) => {
      let innerOffsetTop = 0;
      do {
        if (!isNil(elem!.offsetTop)) {
          innerOffsetTop += elem!.offsetTop;
        }
      } while ((elem = elem!.offsetParent as HTMLElement));
      return innerOffsetTop;
    };
    const offsetTop = getOffsetTop(this.baseElement.nativeElement);
    const maxResize = 900;
    let newTop = e.pageY - offsetTop;
    if (newTop > maxResize) {
      newTop = maxResize;
    }
    if (newTop < this.resizeMin) {
      newTop = this.resizeMin;
    }
    return newTop;
  }

  /**
   * Get recursive left
   * @param e
   */
  getResizeLeft(e: MouseEvent): number {
    const getOffsetLeft = (elem: HTMLElement | null) => {
      let innerOffsetLeft = 0;
      do {
        if (!isNil(elem!.offsetLeft)) {
          innerOffsetLeft += elem!.offsetLeft;
        }
      } while ((elem = elem!.offsetParent as HTMLElement));
      return innerOffsetLeft;
    };
    const offsetLeft = getOffsetLeft(this.baseElement.nativeElement);
    const maxResize = this.baseElement.nativeElement.getBoundingClientRect().width - 200;
    let newLeft = e.pageX - offsetLeft;
    if (newLeft > maxResize) {
      newLeft = maxResize;
    }
    if (newLeft < this.resizeMin) {
      newLeft = this.resizeMin;
    }
    return newLeft;
  }

  constructor(@Inject(DOCUMENT) private document: any) {}

  ngOnInit(): void {
    fromEvent<MouseEvent>(this.document, 'mouseup')
      .pipe(takeUntil(this.destroy$))
      .subscribe(e => {
        this.isMoving = false;
        if (e.target === this.trigger.nativeElement) {
          e.stopPropagation();
          this.resizeEnd.emit({ left: this.left, top: this.top });
        }
      });
    fromEvent<MouseEvent>(this.document, 'mousemove')
      .pipe(takeUntil(this.destroy$))
      .subscribe(e => {
        if (!this.isMoving) {
          return;
        }
        e.preventDefault();
        if (this.mode === ResizeModeEnums.Vertical) {
          this.left = this.getResizeLeft(e);
          this.leftChange.emit(this.left);
        } else {
          this.top = this.getResizeTop(e);
          this.topChange.emit(this.top);
        }
      });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
