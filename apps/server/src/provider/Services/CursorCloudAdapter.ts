/**
 * CursorCloudAdapter — shape type for the Cursor Cloud provider adapter.
 *
 * @module CursorCloudAdapter
 */
import type { ProviderAdapterError } from "../Errors.ts";
import type { ProviderAdapterShape } from "./ProviderAdapter.ts";

export interface CursorCloudAdapterShape extends ProviderAdapterShape<ProviderAdapterError> {}
