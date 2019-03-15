import { Component, ChangeDetectionStrategy, Input, Output, EventEmitter } from '@angular/core';

@Component({
  selector       : 'flink-refresh-download',
  templateUrl    : './refresh-download.component.html',
  styleUrls      : [ './refresh-download.component.less' ],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class RefreshDownloadComponent {
  @Input() downloadName: string;
  @Input() downloadHref: string;
  @Output() reload = new EventEmitter<void>();
}
